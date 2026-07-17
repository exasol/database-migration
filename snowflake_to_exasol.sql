create schema if not exists database_migration;

/*
    snowflake_to_exasol.sql  -  generate the statements to migrate a Snowflake database to Exasol v8.

    Source: Snowflake (verified on Snowflake 10.24, JDBC driver 4.3.2). This script runs on the TARGET Exasol
    database, reads the SOURCE metadata through a Snowflake JDBC connection, and RETURNS the statements (CREATE SCHEMA
    / CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / COMMENTs / IMPORT / a final CONSTRAINT STATE section / optional
    VIEW review section / optional DATA VALIDATION). It changes nothing itself - review the output and run it in the
    order returned.

    Snowflake is three-level (database.schema.table); Exasol is two-level (schema.table). FLATTEN_DB_TO_SCHEMA=false
    maps a Snowflake schema to an Exasol schema of the same name; =true maps it to "<database>_<schema>" (collision-safe
    across multiple databases). DB_FILTER selects databases by name/pattern against everything the connection's role can
    see; the only automatic exclusions are Snowflake's internal SNOWFLAKE application DB (which INFORMATION_SCHEMA does
    not list) and every INFORMATION_SCHEMA schema. Shares / imported / personal databases (e.g. SNOWFLAKE_SAMPLE_DATA)
    ARE migrated when they match DB_FILTER.

    DATA TYPE MAPPING (every Snowflake type CREATE-probed live on 10.24; every transfer verified through IMPORT FROM
    JDBC). Snowflake's INFORMATION_SCHEMA collapses spellings to a canonical DATA_TYPE:
      NUMBER(p,s) -> DECIMAL(p,s) (p>36 -> DECIMAL_OVERFLOW; Snowflake has no negative scale; INT/BIGINT/... are
      NUMBER(38,0) and therefore go through DECIMAL_OVERFLOW); FLOAT -> DOUBLE (inf/-inf/NaN -> NULL, Exasol has none);
      TEXT -> VARCHAR(min(len,2000000)) UTF8; BINARY -> VARCHAR hex (BINARY_HANDLING); BOOLEAN -> BOOLEAN; DATE -> DATE;
      TIME(p) -> VARCHAR (Exasol has no TIME type); TIMESTAMP_NTZ/DATETIME(p) -> TIMESTAMP(min(p,9));
      TIMESTAMP_LTZ/TIMESTAMP_TZ(p) -> TIMESTAMP(min(p,9)) normalized to UTC; VARIANT/OBJECT/ARRAY/MAP -> VARCHAR JSON
      text (compact, via TO_JSON); GEOGRAPHY/GEOMETRY -> VARCHAR (WKT, via ST_ASTEXT); VECTOR -> VARCHAR JSON array.
      Anything else (e.g. FILE) -> VARCHAR(2000000) catch-all (never silently dropped).

    Because neither NUMBER(p>36), BINARY, TIMESTAMP_TZ, VECTOR nor FLOAT inf/-inf transfer raw over JDBC, the generated
    IMPORT converts them on the Snowflake side (CAST / HEX_ENCODE / CONVERT_TIMEZONE / TO_JSON / ST_ASTEXT / a finite
    guard) so the transferred value is a plain scalar Exasol accepts. Timestamps preserve full nanosecond precision
    (Exasol TIMESTAMP(9)); TIMESTAMP_LTZ/TIMESTAMP_TZ are normalized to UTC.

    Hard limits (fail loudly, never corrupt): NUMBER needing > 36 digits under DECIMAL_OVERFLOW='CAP'; a text/JSON/hex
    value > 2,000,000 chars (unless TRUNCATE_LONG_STRINGS=true).

    CONSTRAINTS: Snowflake PK/FK are informational (not enforced) and their columns are not in INFORMATION_SCHEMA; they
    are read via SHOW PRIMARY/IMPORTED KEYS, migrated and created DISABLED, then set per CONSTRAINT_STATE after the load.

    Not migrated (out of scope): indexes, UNIQUE/CHECK constraints, sequences, defaults, stages, streams, tasks,
    procedures/functions; identity/autoincrement columns are migrated as plain columns.
*/
--/
create or replace script database_migration.SNOWFLAKE_TO_EXASOL(
  CONNECTION_NAME               -- name of the Snowflake JDBC connection inside Exasol -> e.g. SNOWFLAKE_TESTDB
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,DB_FILTER                    -- Snowflake database(s): 'MYDB', 'DB%', 'D1, D2', '%' (every database the role can see, incl. shares like SNOWFLAKE_SAMPLE_DATA; the internal SNOWFLAKE app DB is skipped)
  ,SCHEMA_FILTER                -- schema(s): 'MYSCHEMA', 'APP%', 'S1, S2', '%' (all; INFORMATION_SCHEMA always excluded)
  ,TABLE_FILTER                 -- table(s): 'MY_TABLE', 'MY%', 'T1, T2', '%' (all base tables)
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = derive from the source (see FLATTEN_DB_TO_SCHEMA)
  ,FLATTEN_DB_TO_SCHEMA         -- false (recommended) => Exasol schema = <schema>; true => Exasol schema = <database>_<schema> (multi-DB collision-safe)
  ,PARALLEL_STATEMENTS          -- 'AUTO' (Exasol VCPU/NODES/2, even, clamped 4..64), a positive integer, or 1 (no split). Each table's IMPORT is split into that many parallel STATEMENT clauses via HASH(*) bucketing (exact 1:1). Best on multi-node Exasol + large tables.
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate Snowflake table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; NUMBER>36 -> DECIMAL(36,s), fail-loud on real overflow), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
  ,BINARY_HANDLING              -- 'HEX' (recommended; BINARY migrated as hex text via HEX_ENCODE) or 'SKIP' (load NULL)
  ,TRUNCATE_LONG_STRINGS        -- true: text/JSON/hex values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,CHECK_MIGRATION              -- true/false: additionally emit data-validation metrics (per-table "<table>_MIG_CHK" + a "<schema>_MIG_CHK" summary). Run AFTER the IMPORTs.
) RETURNS TABLE
AS

-- ---- parameter handling -----------------------------------------------------------------------------
ci       = (IDENTIFIER_CASE_INSENSITIVE == true) or (string.upper(tostring(IDENTIFIER_CASE_INSENSITIVE)) == 'TRUE')
flatten  = (FLATTEN_DB_TO_SCHEMA == true) or (string.upper(tostring(FLATTEN_DB_TO_SCHEMA)) == 'TRUE')
gen_comments = (GENERATE_COMMENTS == true) or (string.upper(tostring(GENERATE_COMMENTS)) == 'TRUE')
gen_views    = (GENERATE_VIEWS == true) or (string.upper(tostring(GENERATE_VIEWS)) == 'TRUE')
trunc        = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
gen_check    = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')
cstate = string.upper(tostring(CONSTRAINT_STATE))
if cstate ~= 'SET_AS_SOURCE' and cstate ~= 'FORCE_ENABLE' then cstate = 'FORCE_DISABLE' end
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' and decof ~= 'VARCHAR' then decof = 'CAP' end
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'HEX' end

-- ---- PARALLEL_STATEMENTS: 'AUTO' (Exasol VCPU/NODES/2, even, clamped 4..64), a positive integer, or 1 (no split) ----
ps = 1
ps_note = 'fixed -> 1 (no split)'
do
	local pv = PARALLEL_STATEMENTS
	local is_auto = (pv == null) or (type(pv) == 'string' and (string.upper(pv) == 'AUTO' or pv == ''))
	if is_auto then
		local suc_a, r_a = pquery([[select floor(VCPU/NODES/2) as PS from EXA_STATISTICS.EXA_SYSTEM_EVENTS where EVENT_TYPE = 'STARTUP' order by MEASURE_TIME desc limit 1]])
		if suc_a and #r_a == 1 and r_a[1][1] ~= null then
			ps = math.floor(tonumber(r_a[1][1]))
			if ps % 2 == 1 then ps = ps - 1 end     -- make it an even number
			if ps < 4 then ps = 4 end                -- minimum 4
			if ps > 64 then ps = 64 end              -- maximum 64
			ps_note = 'AUTO -> '..ps..' (Exasol VCPU/NODES/2, even, clamped 4..64)'
		else
			ps = 4
			ps_note = 'AUTO -> 4 (fallback; EXA_STATISTICS.EXA_SYSTEM_EVENTS not readable)'
		end
	else
		local n = tonumber(pv)
		if n ~= nil then ps = math.max(1, math.floor(n)); ps_note = 'fixed -> '..ps
		else ps = 1; ps_note = 'fixed -> 1 (no split)' end
	end
end
-- SQL fragment for the IMPORT source: ps=1 -> plain table; ps>1 -> HASH(*) bucket k of ps (exhaustive + disjoint).
-- MOD(MOD(HASH(*),ps)+ps,ps) keeps the bucket in [0,ps) and avoids the ABS(min int64) overflow.
if ps > 1 then
	imp_from = [['(select *, hash(*) as "__PBKT__" from "' || "db_name" || '"."' || "schema_name" || '"."' || "table_name" || '") where mod(mod("__PBKT__", ]]..ps..[[) + ]]..ps..[[, ]]..ps..[[) = ' || "k"]]
else
	imp_from = [['"' || "db_name" || '"."' || "schema_name" || '"."' || "table_name" || '"']]
end

exa_upper_begin='' exa_upper_end=''
if ci then exa_upper_begin='upper(' exa_upper_end=')' end
function U(col) return exa_upper_begin..col..exa_upper_end end
function fold(s) if ci then return string.upper(s) else return s end end
function esc(s) return (s:gsub("'", "''")) end

if cstate == 'FORCE_ENABLE' then sw='ENABLE'  scomment=[[  -- forced ENABLE (Exasol validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw='DISABLE'  scomment=[[  -- Snowflake PK/FK are informational (not enforced) -> kept DISABLED]]
else sw='DISABLE'  scomment=[[  -- forced DISABLE (metadata only; faster)]] end

-- ---- filters ----------------------------------------------------------------------------------------
function flt(val)
	if string.match(val, '%%') then
		return [[ like '']]..val..[['']]
	else
		return [[ in ('']]..val:gsub("^%s*(.-)%s*$","%1"):gsub('%s*,%s*',"'',''")..[['')]]
	end
end
DBF = flt(DB_FILTER)
SF  = flt(SCHEMA_FILTER)
TF  = flt(TABLE_FILTER)

-- ---- Exasol target schema expression (used in the generated SQL) ------------------------------------
if TARGET_SCHEMA ~= null and TARGET_SCHEMA ~= '' then
	sname_sql = U([[']]..TARGET_SCHEMA..[[']])
elseif flatten then
	sname_sql = U([["db_name" || '_' || "schema_name"]])
else
	sname_sql = U([["schema_name"]])
end
function exa_schema_of(db, sch)
	local base
	if TARGET_SCHEMA ~= null and TARGET_SCHEMA ~= '' then base = TARGET_SCHEMA
	elseif flatten then base = db..'_'..sch
	else base = sch end
	return fold(base)
end

-- ---- database list (honours DB_FILTER against every database the role can see; INFORMATION_SCHEMA.DATABASES does
--      not list Snowflake's internal SNOWFLAKE application DB, so that one is naturally skipped. Shares / imported /
--      personal databases ARE included when they match DB_FILTER - name them explicitly or via '%'). ----------------
suc0, res1 = pquery([[select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement
	'select database_name from information_schema.databases where database_name ]]..DBF..[[ order by database_name') d ("db")]])
if not suc0 then error('Could not read the Snowflake database list: '..res1.error_message) end
if #res1 < 1 then error('No Snowflake database matched DB_FILTER = '..tostring(DB_FILTER)) end

-- ---- per-database metadata query (UNION ALL across matched databases) -------------------------------
mq = ''
for i=1,#res1 do
	local db = res1[i][1]
	local piece = [[select '']]..db..[['' as db_name, c.table_schema, c.table_name, cast(c.ordinal_position as number(9,0)) as ordinal_position, c.column_name, c.data_type, cast(c.numeric_precision as number(9,0)) as numeric_precision, cast(c.numeric_scale as number(9,0)) as numeric_scale, cast(c.character_maximum_length as number(18,0)) as character_maximum_length, cast(c.datetime_precision as number(9,0)) as datetime_precision, c.is_nullable, coalesce(c.comment,'''') as col_comment from "]]..db..[[".information_schema.columns c join "]]..db..[[".information_schema.tables t on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_type = ''BASE TABLE'' and c.table_schema ]]..SF..[[ and c.table_name ]]..TF..[[ and c.table_schema <> ''INFORMATION_SCHEMA'']]
	if i > 1 then mq = mq..[[ union all ]] end
	mq = mq..piece
end
meta_import = [[import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..mq..[[']]

-- ---- constraints via SHOW PRIMARY/IMPORTED KEYS (columns are not in INFORMATION_SCHEMA) --------------
pk_by = {}
fk_by = {}
for i=1,#res1 do
	local db = res1[i][1]
	local sp, rp = pquery([[select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'show primary keys in database "]]..db..[["')]])
	if sp then
		for k=1,#rp do
			local sch=rp[k][3] local tbl=rp[k][4] local col=rp[k][5] local seq=tonumber(rp[k][6])
			local key=db..'|'..sch..'|'..tbl
			if not pk_by[key] then pk_by[key]={db=db,sch=sch,tbl=tbl,cols={}} end
			pk_by[key].cols[#pk_by[key].cols+1]={seq=seq,col=col}
		end
	end
	local sf2, rf = pquery([[select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'show imported keys in database "]]..db..[["')]])
	if sf2 then
		for k=1,#rf do
			local pk_sch=rf[k][3] local pk_tbl=rf[k][4] local pk_col=rf[k][5]
			local pk_db=rf[k][2]
			local fk_sch=rf[k][7] local fk_tbl=rf[k][8] local fk_col=rf[k][9]
			local seq=tonumber(rf[k][10]) local fkname=rf[k][13]
			local key=db..'|'..fk_sch..'|'..fkname
			if not fk_by[key] then fk_by[key]={db=db,fk_sch=fk_sch,fk_tbl=fk_tbl,r_db=pk_db,r_sch=pk_sch,r_tbl=pk_tbl,name=fkname,cols={}} end
			fk_by[key].cols[#fk_by[key].cols+1]={seq=seq,fkcol=fk_col,rcol=pk_col}
		end
	end
end

pk_vals = {}
for key,v in pairs(pk_by) do
	table.sort(v.cols, function(a,b) return a.seq<b.seq end)
	local cl=''
	for j=1,#v.cols do if j>1 then cl=cl..', ' end cl=cl..'"'..fold(v.cols[j].col)..'"' end
	local es=exa_schema_of(v.db,v.sch) local et=fold(v.tbl)
	local ddl='ALTER TABLE "'..es..'"."'..et..'" ADD CONSTRAINT "'..et..'_PK" PRIMARY KEY ('..cl..') DISABLE;'
	local st ='ALTER TABLE "'..es..'"."'..et..'" MODIFY CONSTRAINT "'..et..'_PK" '..sw..';'..scomment
	pk_vals[#pk_vals+1]="('"..esc(v.sch).."', '"..esc(v.tbl).."', '"..esc(ddl).."', '"..esc(st).."')"
end
fk_vals = {}
for key,v in pairs(fk_by) do
	table.sort(v.cols, function(a,b) return a.seq<b.seq end)
	local fcl='' local rcl=''
	for j=1,#v.cols do if j>1 then fcl=fcl..', ' rcl=rcl..', ' end fcl=fcl..'"'..fold(v.cols[j].fkcol)..'"' rcl=rcl..'"'..fold(v.cols[j].rcol)..'"' end
	local es=exa_schema_of(v.db,v.fk_sch) local et=fold(v.fk_tbl)
	local res_es=exa_schema_of(v.r_db,v.r_sch) local res_et=fold(v.r_tbl) local fn=fold(v.name)
	local ddl='ALTER TABLE "'..es..'"."'..et..'" ADD CONSTRAINT "'..fn..'" FOREIGN KEY ('..fcl..') REFERENCES "'..res_es..'"."'..res_et..'" ('..rcl..') DISABLE;'
	local st ='ALTER TABLE "'..es..'"."'..et..'" MODIFY CONSTRAINT "'..fn..'" '..sw..';'..scomment
	fk_vals[#fk_vals+1]="('"..esc(v.fk_sch).."', '"..esc(v.fk_tbl).."', '"..esc(ddl).."', '"..esc(st).."')"
end
if #pk_vals==0 then pk_src=[[select cast(null as varchar(2000000)) "s_schema", cast(null as varchar(2000000)) "s_table", cast(null as varchar(2000000)) "sql_text", cast(null as varchar(2000000)) "state_text" from dual where 1=0]]
else pk_src=[[select * from values ]]..table.concat(pk_vals,', ')..[[ as t("s_schema","s_table","sql_text","state_text")]] end
if #fk_vals==0 then fk_src=[[select cast(null as varchar(2000000)) "s_schema", cast(null as varchar(2000000)) "s_table", cast(null as varchar(2000000)) "sql_text", cast(null as varchar(2000000)) "state_text" from dual where 1=0]]
else fk_src=[[select * from values ]]..table.concat(fk_vals,', ')..[[ as t("s_schema","s_table","sql_text","state_text")]] end

-- ---- type mapping (Exasol target) and Snowflake-side read expressions -------------------------------
if decof == 'DOUBLE' then num_over_t=[['DOUBLE']] num_over_src=[['to_double("' || "column_name" || '")']]
elseif decof == 'VARCHAR' then num_over_t=[['VARCHAR(50) ASCII']] num_over_src=[['to_varchar("' || "column_name" || '")']]
else num_over_t=[['DECIMAL(36,' || least("numeric_scale",36) || ')']] num_over_src=[['to_varchar("' || "column_name" || '")']] end

col_t = [[case "dt"
	when 'NUMBER' then case when "numeric_precision" > 36 then ]]..num_over_t..[[ else 'DECIMAL(' || "numeric_precision" || ',' || "numeric_scale" || ')' end
	when 'FLOAT' then 'DOUBLE'
	when 'TEXT' then 'VARCHAR(' || (case when "char_len" is null or "char_len" > 2000000 or "char_len" < 1 then 2000000 else "char_len" end) || ') UTF8'
	when 'BINARY' then 'VARCHAR(2000000) ASCII'
	when 'BOOLEAN' then 'BOOLEAN'
	when 'DATE' then 'DATE'
	when 'TIME' then 'VARCHAR(18) ASCII'
	when 'TIMESTAMP_NTZ' then 'TIMESTAMP(' || (case when "dtp" is null or "dtp" > 9 then 9 else "dtp" end) || ')'
	when 'TIMESTAMP_LTZ' then 'TIMESTAMP(' || (case when "dtp" is null or "dtp" > 9 then 9 else "dtp" end) || ')'
	when 'TIMESTAMP_TZ' then 'TIMESTAMP(' || (case when "dtp" is null or "dtp" > 9 then 9 else "dtp" end) || ')'
	when 'VARIANT' then 'VARCHAR(2000000) UTF8'
	when 'OBJECT' then 'VARCHAR(2000000) UTF8'
	when 'ARRAY' then 'VARCHAR(2000000) UTF8'
	when 'MAP' then 'VARCHAR(2000000) UTF8'
	when 'GEOGRAPHY' then 'VARCHAR(2000000) UTF8'
	when 'GEOMETRY' then 'VARCHAR(2000000) UTF8'
	when 'VECTOR' then 'VARCHAR(2000000) ASCII'
	else 'VARCHAR(2000000) UTF8'
end]]

if binmode == 'SKIP' then bin_src=[['cast(null as varchar)']]
elseif trunc then bin_src=[['substr(hex_encode("' || "column_name" || '"), 1, 2000000)']]
else bin_src=[['hex_encode("' || "column_name" || '")']] end
if trunc then text_src=[['substr("' || "column_name" || '", 1, 2000000)']] else text_src=[['"' || "column_name" || '"']] end
if trunc then json_wrap_a='substr(' json_wrap_b=', 1, 2000000)' else json_wrap_a='' json_wrap_b='' end

src = [[case "dt"
	when 'NUMBER' then case when "numeric_precision" > 36 then ]]..num_over_src..[[ else 'to_varchar("' || "column_name" || '")' end
	when 'FLOAT' then 'case when "' || "column_name" || '" = ''inf''::float or "' || "column_name" || '" = ''-inf''::float or "' || "column_name" || '" != "' || "column_name" || '" then null else "' || "column_name" || '" end'
	when 'TEXT' then ]]..text_src..[[
	when 'BINARY' then ]]..bin_src..[[
	when 'BOOLEAN' then '"' || "column_name" || '"'
	when 'DATE' then '"' || "column_name" || '"'
	when 'TIME' then 'to_char("' || "column_name" || '", ''HH24:MI:SS.FF9'')'
	when 'TIMESTAMP_NTZ' then '"' || "column_name" || '"'
	when 'TIMESTAMP_LTZ' then 'convert_timezone(''UTC'', "' || "column_name" || '")::timestamp_ntz'
	when 'TIMESTAMP_TZ' then 'convert_timezone(''UTC'', "' || "column_name" || '")::timestamp_ntz'
	when 'VARIANT' then ']]..json_wrap_a..[[to_json(cast("' || "column_name" || '" as variant))]]..json_wrap_b..[['
	when 'OBJECT' then ']]..json_wrap_a..[[to_json(cast("' || "column_name" || '" as variant))]]..json_wrap_b..[['
	when 'ARRAY' then ']]..json_wrap_a..[[to_json(cast("' || "column_name" || '" as variant))]]..json_wrap_b..[['
	when 'MAP' then ']]..json_wrap_a..[[to_json(cast("' || "column_name" || '" as variant))]]..json_wrap_b..[['
	when 'GEOGRAPHY' then 'st_astext("' || "column_name" || '")'
	when 'GEOMETRY' then 'st_astext("' || "column_name" || '")'
	when 'VECTOR' then 'to_json("' || "column_name" || '"::array)'
	else 'to_varchar("' || "column_name" || '")'
end]]

known = [["dt" in ('NUMBER','FLOAT','TEXT','BINARY','BOOLEAN','DATE','TIME','TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ','VARIANT','OBJECT','ARRAY','MAP','GEOGRAPHY','GEOMETRY','VECTOR')]]
notnull_ok = [["dt" in ('NUMBER','BOOLEAN','DATE','TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ')]]

-- ---- optional CTEs: comments, views -----------------------------------------------------------------
comments_cte='' comments_union=''
if gen_comments then
	comments_cte = [[
,vv_tabcomm_raw as (select * from (]]..meta_import..[[) t ("db_name","schema_name","table_name","ordinal_position","column_name","data_type","numeric_precision","numeric_scale","character_maximum_length","datetime_precision","is_nullable","comment_text"))
,vv_comment_col as (select 'COMMENT ON COLUMN "' || ]]..sname_sql..[[ || '"."' || ]]..U('"table_name"')..[[ || '"."' || ]]..U('"column_name"')..[[ || '" IS ''' || replace("comment_text", '''', '''''') || ''';' as sql_text from vv_tabcomm_raw where "comment_text" is not null and "comment_text" <> '')]]
	comments_union = "\n"..[[UNION ALL select 41, cast('-- ### COLUMN COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte='' views_union=''
if gen_views then
	vmq=''
	for i=1,#res1 do
		local db=res1[i][1]
		local piece=[[select '']]..db..[['' as db_name, table_schema, table_name, view_definition from "]]..db..[[".information_schema.views where table_schema ]]..SF..[[ and table_name ]]..TF..[[ and table_schema <> ''INFORMATION_SCHEMA'']]
		if i>1 then vmq=vmq..[[ union all ]] end
		vmq=vmq..piece
	end
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..vmq..[[') v ("db_name","schema_name","view_name","view_def"))
,vv_views as (select '-- ' || "db_name" || '.' || "schema_name" || '.' || "view_name" || '  - Snowflake view, review and adapt to Exasol SQL manually' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n"..[[UNION ALL select 90, cast('-- ### VIEWS (Snowflake SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

-- ---- CHECK_MIGRATION (create-then-insert wide metrics + per-schema summary) --------------------------
check_cte='' check_union=''
if gen_check then
	chk_num = [["dt"='NUMBER' and "numeric_scale" is not null and "numeric_scale" between 0 and 36 and (case when "numeric_precision" is null then 38 else "numeric_precision" end) <= 36]]
	chk_dt  = [[("dt"='DATE' or "dt"='TIMESTAMP_NTZ')]]
	exact_ok= [[(]]..chk_num..[[ or "dt"='BOOLEAN' or ]]..chk_dt..[[)]]
	sc = [["numeric_scale"]]
	check_cte = [[
,vv_chk_cols as (select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord" from vv_columns x where ]]..exact_ok..[[ or "ordinal_position" = (select min(y."ordinal_position") from vv_columns y where y."exa_schema"=x."exa_schema" and y."exa_table"=x."exa_table"))
,vv_chk_x as (
	select c.*, sysrow."db_system", mid."metric_id",
	   case when sysrow."db_system"='Exasol' then '"' || c."exa_col" || '"' else '"' || c."column_name" || '"' end as "ref"
	from vv_chk_cols c
	cross join (select 'Exasol' as "db_system" union all select 'SNOWFLAKE' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 6) mid
)
,vv_chk_e as (
	select "exa_schema","exa_table","db_name","schema_name","table_name","exa_col","column_name","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide",
	   (case
	      when "metric_id"=0 and "ordinal_position"="min_ord" then (case when "db_system"='Exasol' then 'cast(count(*) as decimal(36,0))' else 'to_char(cast(count(*) as number(36,0)))' end)
	      when "metric_id"=1 and ]]..exact_ok..[[ then (case when "db_system"='Exasol' then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))' else 'to_char(cast(count(case when ' || "ref" || ' is null then 1 end) as number(36,0)))' end)
	      when "metric_id"=2 and (]]..chk_num..[[) then (case when "db_system"='Exasol' then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'to_char(cast(min(' || "ref" || ') as number(36,' || ]]..sc..[[ || ')))' end)
	      when "metric_id"=2 and "dt"='DATE' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD'')'
	      when "metric_id"=2 and "dt"='TIMESTAMP_NTZ' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF9'')'
	      when "metric_id"=3 and (]]..chk_num..[[) then (case when "db_system"='Exasol' then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'to_char(cast(max(' || "ref" || ') as number(36,' || ]]..sc..[[ || ')))' end)
	      when "metric_id"=3 and "dt"='DATE' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD'')'
	      when "metric_id"=3 and "dt"='TIMESTAMP_NTZ' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF9'')'
	      when "metric_id"=4 and (]]..chk_num..[[) then (case when "db_system"='Exasol' then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'to_char(cast(sum(' || "ref" || ') as number(36,' || ]]..sc..[[ || ')))' end)
	      when "metric_id"=5 and ]]..exact_ok..[[ then (case when "db_system"='Exasol' then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))' else 'to_char(cast(count(distinct ' || "ref" || ') as number(36,0)))' end)
	    end) as "mexpr",
	   (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_SUM' when 5 then "exa_col" || '_DISTINCT' end) as "mname"
	from vv_chk_x
)
,vv_chk_named as (select * from vv_chk_e where "mexpr" is not null)
,vv_chk_sys as (
	select "exa_schema","exa_table","db_name","schema_name","table_name","wide","db_system",
	   case when "db_system"='Exasol'
	     then 'select cast(''Exasol'' as varchar(20)) as "DB_SYSTEM", ' || group_concat("mexpr" || ' as "' || "mname" || '"' order by "ordinal_position","metric_id" separator ', ') || ' from "' || "exa_schema" || '"."' || "exa_table" || '"'
	     else 'select ''SNOWFLAKE'' as "DB_SYSTEM", x.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ' || '''' || replace('select ' || group_concat("mexpr" order by "ordinal_position","metric_id" separator ', ') || ' from "' || "db_name" || '"."' || "schema_name" || '"."' || "table_name" || '"', '''', '''''') || '''' || ') x'
	   end as "sel"
	from vv_chk_named group by "exa_schema","exa_table","db_name","schema_name","table_name","wide","db_system"
)
,vv_chk_create as (select 'create or replace table "' || "exa_schema" || '"."' || "wide" || '" as ' || "sel" || ';' as sql_text from vv_chk_sys where "db_system"='Exasol')
,vv_chk_insert as (select 'insert into "' || "exa_schema" || '"."' || "wide" || '" ' || "sel" || ';' as sql_text from vv_chk_sys where "db_system"='SNOWFLAKE')
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system","wide","mname",
	   'select ''' || "exa_table" || ''' as "TABLE_NAME", ''' || "mname" || ''' as "METRIC", to_char("' || "mname" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide" || '" where "DB_SYSTEM" = ''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'create or replace table "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" as select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", o."VAL" as "SNOWFLAKE_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(o."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || group_concat(case when "db_system"='Exasol' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') e join (' || group_concat(case when "db_system"='SNOWFLAKE' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') o on e."TABLE_NAME"=o."TABLE_NAME" and e."METRIC"=o."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
	from vv_chk_unpiv group by "exa_schema"
)]]
	check_union = "\n"..[[UNION ALL select 70, cast('-- ### DATA VALIDATION (CHECK_MIGRATION) - run AFTER the IMPORTs; compares source vs target metrics ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 71, sql_text from vv_chk_create
UNION ALL select 72, sql_text from vv_chk_insert
UNION ALL select 73, cast('-- per-schema validation summary - one row per metric, STATUS = OK / DEVIATION' as varchar(2000000))
UNION ALL select 74, sql_text from vv_chk_summary
UNION ALL select 75, cast('-- review deviations with:  select * from "DATABASE_MIGRATION"."<schema>_MIG_CHK" where "STATUS" = ''DEVIATION'';' as varchar(2000000))]]
end

-- ---- main query -------------------------------------------------------------------------------------
suc, res = pquery([[
with vv_columns as (
	select ]]..sname_sql..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table", ]]..U('"column_name"')..[[ as "exa_col",
	       upper(trim("data_type")) as "dt",
	       case when "is_nullable" = 'NO' then 1 else 0 end as "not_null",
	       cast("numeric_precision" as decimal(18,0)) as "numeric_precision",
	       cast("numeric_scale" as decimal(18,0)) as "numeric_scale",
	       cast("character_maximum_length" as decimal(18,0)) as "char_len",
	       cast("datetime_precision" as decimal(18,0)) as "dtp",
	       cast("ordinal_position" as decimal(9,0)) as "ordinal_position",
	       "db_name","schema_name","table_name","column_name"
	from (]]..meta_import..[[) t ("db_name","schema_name","table_name","ordinal_position","column_name","data_type","numeric_precision","numeric_scale","character_maximum_length","datetime_precision","is_nullable","comment_text")
)
,vv_catchall as (
	select '-- NOTE: column "' || "db_name" || '"."' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" has type ' || "dt" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
	from vv_columns where not (]]..known..[[)
)
,vv_pk as (select p."sql_text" as sql_text, p."state_text" as state_text from (]]..pk_src..[[) p where exists (select 1 from vv_columns c where c."schema_name"=p."s_schema" and c."table_name"=p."s_table"))
,vv_fk as (select f."sql_text" as sql_text, f."state_text" as state_text from (]]..fk_src..[[) f where exists (select 1 from vv_columns c where c."schema_name"=f."s_schema" and c."table_name"=f."s_table"))
,vv_create_schemas as (select distinct 'CREATE SCHEMA IF NOT EXISTS "' || "exa_schema" || '";' as sql_text from vv_columns)
,vv_create_tables as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "exa_table" || '" (' || group_concat('"' || "exa_col" || '" ' || (]]..col_t..[[) || (case when "not_null"=1 and (]]..notnull_ok..[[) then ' NOT NULL' else '' end) order by "ordinal_position" separator ', ') || ');' as sql_text
	from vv_columns group by "exa_schema","exa_table"
)
,vv_cl as (
	select "exa_schema","db_name","schema_name","exa_table","table_name",
	       group_concat('"' || "exa_col" || '"' order by "ordinal_position" separator ', ') as collist,
	       group_concat((]]..src..[[) order by "ordinal_position" separator ', ') as srclist
	from vv_columns group by "exa_schema","db_name","schema_name","exa_table","table_name"
)
,vv_nums as (select level-1 as "k" from dual connect by level <= ]]..ps..[[)
,vv_imports as (
	select 'IMPORT INTO "' || "exa_schema" || '"."' || "exa_table" || '" (' || min(collist) || ') FROM JDBC AT ]]..CONNECTION_NAME..[[' ||
	       group_concat(' STATEMENT ' || '''' || replace('select ' || srclist || ' from ' || ]]..imp_from..[[, '''', '''''') || '''' order by "k" separator '') || ';' as sql_text
	from vv_cl cross join vv_nums
	group by "exa_schema","exa_table","db_name","schema_name","table_name"
)]]..comments_cte..views_cte..check_cte..[[
select sql_text from (
	select -3 ord, cast('-- ### Snowflake -> Exasol migration.  TIMESTAMP_TZ/LTZ normalized to UTC; full nanosecond precision preserved (TIMESTAMP(9)). ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select -2, cast('-- character data -> Exasol UTF8; semi-structured (VARIANT/OBJECT/ARRAY/MAP) -> JSON text; GEOGRAPHY/GEOMETRY -> WKT; VECTOR -> JSON array; BINARY -> hex.' as varchar(2000000))
	UNION ALL select (-1.5), cast('-- PARALLEL_STATEMENTS = ]]..ps_note..[[  -> each table IMPORT is split into ]]..ps..[[ parallel STATEMENT clause(s) via HASH(*) bucketing (exact 1:1; verify with CHECK_MIGRATION). Fastest on multi-node Exasol + large tables.' as varchar(2000000))
	UNION ALL select 0, sql_text from vv_catchall
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 3, cast('-- ### TABLES ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk
	UNION ALL select 7, cast('-- ### FOREIGN KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 8, sql_text from vv_fk]]..comments_union..[[
	UNION ALL select 50, cast('-- ### IMPORTS ( ]]..ps..[[ parallel STATEMENT(s) per table ) ###' as varchar(2000000))
	UNION ALL select 51, sql_text from vv_imports
	UNION ALL select 60, cast('-- ### CONSTRAINT STATE - run AFTER the data load ###' as varchar(2000000))
	UNION ALL select 61, state_text from vv_pk
	UNION ALL select 62, state_text from vv_fk]]..views_union..check_union..[[
) order by ord
]],{})

if not suc then error('"'..res.error_message..'" caught while executing: "'..res.statement_text..'"') end
return(res)
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Prerequisites
--   * The Snowflake database must be reachable from this Exasol database.
--
-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * snowflake-jdbc 4.3.1 or higher
--       https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc
--   * Driver setup guide:
--       https://docs.exasol.com/db/latest/loading_data/connect_sources/snowflake.htm
--   * Detailed information about configuring the Snowflake JDBC driver
--       https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure
--
-- Create a connection to the Snowflake database (adjust account, warehouse, database and credentials),
-- then run the accompanying test query.

CREATE OR REPLACE CONNECTION SNOWFLAKE_JDBC
	TO 'jdbc:snowflake://<myorganization>-<myaccount>.snowflakecomputing.com/?warehouse=<my_compute_wh>&db=<my_db>&CLIENT_SESSION_KEEP_ALIVE=true&JDBC_QUERY_RESULT_FORMAT=JSON'
	USER '<user>'
	IDENTIFIED BY '<password_or_token>';
SELECT * FROM (IMPORT FROM JDBC AT SNOWFLAKE_JDBC STATEMENT 'select ''Connection works'' as connection_status');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.SNOWFLAKE_TO_EXASOL(
	'SNOWFLAKE_JDBC',       -- CONNECTION_NAME: Snowflake JDBC connection
	true,                   -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
	'%',                    -- DB_FILTER: Snowflake database(s): 'MYDB', 'DB%', 'D1, D2', '%' (every database the role can see, incl. shares)
	'%',                    -- SCHEMA_FILTER: schema(s): 'MYSCHEMA', 'APP%', 'S1, S2', '%' (all; INFORMATION_SCHEMA excluded)
	'%',                    -- TABLE_FILTER: table(s): 'MY_TABLE', 'MY%', 'T1, T2', '%' (all base tables)
	'',                     -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => derive from source (see FLATTEN_DB_TO_SCHEMA)
	false,                  -- FLATTEN_DB_TO_SCHEMA: false (recommended) => Exasol schema = <schema>; true => <database>_<schema> (multi-DB collision-safe)
	'AUTO',                 -- PARALLEL_STATEMENTS: 'AUTO' (Exasol VCPU/NODES/2, even, 4..64), a positive integer, or 1 (no split). Split per table via HASH(*) bucketing (exact 1:1). Best on multi-node Exasol + large tables.
	'FORCE_DISABLE',        -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK metadata only), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (Exasol validates the data)
	true,                   -- GENERATE_COMMENTS: true (recommended) => migrate Snowflake comments as COMMENT ON; false => skip
	true,                   -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
	'CAP',                  -- DECIMAL_OVERFLOW: 'CAP' (recommended; NUMBER>36 -> DECIMAL(36,s)), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
	'HEX',                  -- BINARY_HANDLING: 'HEX' (recommended; BINARY as hex text) or 'SKIP' (load NULL)
	false,                  -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
	false                   -- CHECK_MIGRATION: false (recommended default) => skip; true => also build "<table>_MIG_CHK" metric tables + a "<schema>_MIG_CHK" summary (source vs target) for post-load validation
);
