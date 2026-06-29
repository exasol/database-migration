create schema if not exists database_migration;

/*
    netezza_to_exasol.sql  -  generate the statements to migrate an IBM Netezza (NPS) database to Exasol v8.

    Source: IBM Netezza Performance Server (NPS) 7.x / 11.x. This script runs on the TARGET Exasol database,
    reads the SOURCE metadata through a JDBC connection (native _V_* catalog) and RETURNS the statements
    (CREATE SCHEMA / CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / DISTRIBUTE BY / COMMENTs / IMPORT / a final
    CONSTRAINT STATE section / optional VIEW review section / optional DATA VALIDATION). It changes nothing
    itself - review the output and run it in the order returned.

    IMPORTANT - CONNECTION: the JDBC connection must point at the SOURCE database to migrate (e.g.
    jdbc:netezza://host:5480/MYDB), NOT at the SYSTEM database. Netezza cannot hold user tables in SYSTEM, and
    the _V_* catalog views are database-scoped, so the script reads the connected database's catalog and imports
    schema-qualified tables ("SCHEMA"."TABLE").

    DATA TYPE MAPPING (EVERY type this NPS supports was CREATE-probed live; FORMAT_TYPE is the catalog's rendered
    type). Reported FORMAT_TYPE -> Exasol:
      BYTEINT -> DECIMAL(3,0); SMALLINT/INTEGER/BIGINT -> DECIMAL(5/10/19,0); NUMERIC(p,s) -> DECIMAL(p,s) (p>36
      -> DECIMAL_OVERFLOW; Netezza max precision 38); REAL/DOUBLE PRECISION (also FLOAT/FLOAT(n)) -> DOUBLE;
      CHARACTER / CHARACTER VARYING / NATIONAL CHARACTER[ VARYING] -> CHAR/VARCHAR UTF8 (char>2000 -> VARCHAR);
      DATE -> DATE; TIMESTAMP -> TIMESTAMP(6); BOOLEAN -> BOOLEAN; JSON / JSONB / JSONPATH -> VARCHAR (text).
      Small documented difference: TIME -> VARCHAR(15), TIME WITH TIME ZONE -> VARCHAR(21) (Exasol has no TIME
      type); INTERVAL -> VARCHAR or best-effort native INTERVAL (INTERVAL_HANDLING); ST_GEOMETRY -> VARCHAR (WKT,
      best-effort); BINARY VARYING (BINARY/VARBINARY) -> VARCHAR hex (BINARY_HANDLING). Anything unexpected ->
      VARCHAR(2000000) catch-all (read directly; fails loudly if untransferable - never a silent drop).
      Hard limits (fail loudly, never corrupt): a NUMERIC needing > 36 digits under DECIMAL_OVERFLOW='CAP'; a
      binary value > 32000 bytes in BINARY_HANDLING='HEX' (Netezza's 64000-char VARCHAR limit on the hex text).
      NOT supported by this NPS (CREATE rejects them, so they cannot occur): MONEY, GRAPHIC/VARGRAPHIC, LONG
      VARCHAR, CLOB/BLOB, BYTE/VARBYTE, TIMESTAMP WITH TIME ZONE, XML, ARRAY, UUID.

    WHY SOME COLUMNS ARE READ WITH A CAST/FUNCTION ON THE SOURCE (verified live with the Netezza JDBC driver):
    the driver cannot transfer some types directly, so the generated IMPORT reads them as text -
      TIME ("Bad value for NZ_TIME") and INTERVAL ("unknown") -> CAST(.. AS VARCHAR);  TIME WITH TIME ZONE -> CAST;
      BINARY VARYING (raw = "unknown") -> to_hex(..);  ST_GEOMETRY (raw + cast both fail) -> ST_ASTEXT(..) (WKT).
    Everything else - integers, NUMERIC, REAL/DOUBLE, all char types incl. NCHAR/NVARCHAR, DATE, TIMESTAMP (full
    microseconds), BOOLEAN, and JSON/JSONB/JSONPATH (raw transfer works) - is read directly. Column DEFAULTs carry
    a Netezza type cast (e.g. 'NEW'::"NVARCHAR") which is stripped.

    INTERNAL DATA TYPES: ROWID, CREATEXID, DELETEXID, DATASLICEID are queryable pseudo-columns but are NOT listed
    in _V_RELATION_COLUMN, so they are never migrated (they are physical/MVCC metadata, not user data).

    TEMPORAL: Netezza stores DATE/TIME/TIMESTAMP internally as integers, but the driver/CAST return calendar
    values, so the migration is by value (verified across the full range, microsecond precision preserved).

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are not
    affected by differing source/target locale settings. Character data is stored as UTF8.

    CONSTRAINTS: Netezza PK/FK are informational (not enforced) but are migrated. PK/FK are created DISABLED;
    a final CONSTRAINT STATE section sets them per CONSTRAINT_STATE.

    DISTRIBUTION (GENERATE_DISTRIBUTION_BY): the Netezza hash distribution key (DISTRIBUTE ON) is mapped to an
    Exasol DISTRIBUTE BY (verified live). Netezza has no range partitioning; ORGANIZE ON (CBT clustering) is not
    mapped.

    Not migrated (out of scope): indexes/zone maps, UNIQUE/CHECK constraints, sequences, procedures, materialized
    views.
*/
--/
create or replace script database_migration.NETEZZA_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol (must point at the SOURCE database) -> e.g. NETEZZA_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended; Netezza folds unquoted names to UPPER) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- filter for the source schemas (system schemas always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate Netezza table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,GENERATE_DISTRIBUTION_BY     -- true/false (default true): add a DISTRIBUTE BY from the Netezza hash distribution key
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; numeric>36 -> DECIMAL(36,s); IMPORT fails for values > 36 digits), 'DOUBLE' (loads, ~15 digits) or 'VARCHAR' (lossless text)
  ,INTERVAL_HANDLING            -- 'VARCHAR' (recommended; interval as lossless text) or 'INTERVAL' (native Exasol INTERVAL DAY TO SECOND, best-effort - day-time intervals only; year/month/sub-second NOT carried)
  ,BINARY_HANDLING              -- 'HEX' (recommended; BINARY/VARBINARY migrated losslessly as hex text via to_hex - max 32000 bytes) or 'SKIP' (load NULL)
  ,CHECK_MIGRATION              -- true/false: additionally emit data-validation metrics (per-table "<table>_MIG_CHK" + a "<schema>_MIG_CHK" summary comparing source vs target). Run AFTER the IMPORTs.
) RETURNS TABLE
AS

exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

cstate = string.upper(tostring(CONSTRAINT_STATE))
if cstate ~= 'SET_AS_SOURCE' and cstate ~= 'FORCE_ENABLE' then cstate = 'FORCE_DISABLE' end
gen_comments = (GENERATE_COMMENTS == true) or (string.upper(tostring(GENERATE_COMMENTS)) == 'TRUE')
gen_views    = (GENERATE_VIEWS == true) or (string.upper(tostring(GENERATE_VIEWS)) == 'TRUE')
gen_dist = true   -- DISTRIBUTE BY defaults to true; only an explicit false/FALSE disables it
if (GENERATE_DISTRIBUTION_BY == false) or (string.upper(tostring(GENERATE_DISTRIBUTION_BY)) == 'FALSE') then gen_dist = false end
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' and decof ~= 'VARCHAR' then decof = 'CAP' end
ivmode = string.upper(tostring(INTERVAL_HANDLING))
if ivmode ~= 'INTERVAL' then ivmode = 'VARCHAR' end
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'HEX' end
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
if TARGET_SCHEMA == null then tschema = [["schema_name"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
if TARGET_SCHEMA == null then ref_sname_e = U('"ref_schema"') else ref_sname_e = sname_e end

-- Netezza filter (system schemas always excluded) - parameterized by the schema/table column refs.
function nzflt(s, tn) return [[ and upper(trim(]]..s..[[)) not in (''DEFINITION_SCHEMA'',''INFORMATION_SCHEMA'') and trim(]]..s..[[) like '']]..SCHEMA_FILTER..[['' and trim(]]..tn..[[) like '']]..TABLE_FILTER..[['' ]] end

-------------------------------------------------------------------------------------------------------
-- Remote (Netezza) metadata queries (current-database _V_* catalog). Inner literals are quote-doubled.
-------------------------------------------------------------------------------------------------------
columns_q = [[select trim(SCHEMA), trim(NAME), trim(ATTNAME), ATTNUM, trim(FORMAT_TYPE), ATTNOTNULL, COLDEFAULT from _V_RELATION_COLUMN where TYPE = ''TABLE'']]..nzflt('SCHEMA','NAME')

pk_q = [[select trim(SCHEMA), trim(RELATION), trim(ATTNAME), CONSEQ from _V_RELATION_KEYDATA where CONTYPE = ''p'']]..nzflt('SCHEMA','RELATION')

fk_q = [[select trim(SCHEMA), trim(RELATION), trim(CONSTRAINTNAME), trim(ATTNAME), CONSEQ, trim(PKSCHEMA), trim(PKRELATION), trim(PKATTNAME) from _V_RELATION_KEYDATA where CONTYPE = ''f'']]..nzflt('SCHEMA','RELATION')

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions producing the generated statement text.
-------------------------------------------------------------------------------------------------------
sc = [[(case when "p2" is null or "p2" < 0 then 0 when "p2" > 36 then 36 else "p2" end)]]
if decof == 'DOUBLE' then
	dec_t = [[case when "p1" is null or "p1" > 36 then 'DOUBLE' else 'DECIMAL(' || "p1" || ',' || ]]..sc..[[ || ')' end]]
elseif decof == 'VARCHAR' then
	dec_t = [[case when "p1" is null or "p1" > 36 then 'VARCHAR(2000000) ASCII' else 'DECIMAL(' || "p1" || ',' || ]]..sc..[[ || ')' end]]
else
	dec_t = [[case when "p1" is null then 'DECIMAL(36,18)' when "p1" > 36 then 'DECIMAL(36,' || ]]..sc..[[ || ')' else 'DECIMAL(' || "p1" || ',' || ]]..sc..[[ || ')' end]]
end
if ivmode == 'INTERVAL' then
	-- native Exasol INTERVAL DAY TO SECOND, built from the day-time components via Netezza EXTRACT (chr() avoids
	-- quote escaping: chr(32)=space, chr(58)=':', chr(48)='0'). Best-effort: year/month components and sub-second
	-- fractions are NOT carried (Exasol has no combined interval type) - use INTERVAL_HANDLING='VARCHAR' for those.
	iv_t = [['INTERVAL DAY(9) TO SECOND(6)']]
	iv_src = [['extract(day from ' || "col_q" || ') || chr(32) || lpad(extract(hour from ' || "col_q" || '),2,chr(48)) || chr(58) || lpad(extract(minute from ' || "col_q" || '),2,chr(48)) || chr(58) || lpad(cast(extract(second from ' || "col_q" || ') as integer),2,chr(48))']]
else
	iv_t = [['VARCHAR(60) ASCII']]
	iv_src = [['cast(' || "col_q" || ' as varchar(60))']]
end

-- Exasol column type, mapped by the parsed Netezza base type (+ p1=precision/length, p2=scale).
col_t = [[case
	when "base" = 'BYTEINT' then 'DECIMAL(3,0)'
	when "base" = 'SMALLINT' then 'DECIMAL(5,0)'
	when "base" = 'INTEGER' then 'DECIMAL(10,0)'
	when "base" = 'BIGINT' then 'DECIMAL(19,0)'
	when "base" = 'NUMERIC' then ]]..dec_t..[[
	when "base" in ('REAL','DOUBLE','DOUBLE PRECISION','FLOAT') then 'DOUBLE'
	when "base" in ('CHARACTER','NATIONAL CHARACTER') then case when "p1" > 2000 then 'VARCHAR(' || "p1" || ') UTF8' else 'CHAR(' || "p1" || ') UTF8' end
	when "base" in ('CHARACTER VARYING','NATIONAL CHARACTER VARYING') then 'VARCHAR(' || (case when "p1" is null or "p1" > 2000000 then 2000000 else "p1" end) || ') UTF8'
	when "base" = 'DATE' then 'DATE'
	when "base" = 'TIME' then 'VARCHAR(15) ASCII'
	when "base" = 'TIME WITH TIME ZONE' then 'VARCHAR(21) ASCII'
	when "base" = 'TIMESTAMP' then 'TIMESTAMP(6)'
	when "base" = 'INTERVAL' then ]]..iv_t..[[
	when "base" = 'BOOLEAN' then 'BOOLEAN'
	when "base" = 'ST_GEOMETRY' then 'VARCHAR(2000000) ASCII'
	when "base" = 'BINARY VARYING' then 'VARCHAR(' || (case when "p1" is null or "p1" * 2 > 2000000 then 2000000 else "p1" * 2 end) || ') ASCII'
	when "base" in ('JSON','JSONB','JSONPATH') then 'VARCHAR(2000000) UTF8'
	else 'VARCHAR(2000000) UTF8'
end]]

-- DEFAULT mapping (Netezza COLDEFAULT carries a ::"TYPE" cast which is stripped; nextval/sequence skipped).
default_e = [[case
	when "default_value" is null then ''
	when "default_value" like 'nextval(%' then ''
	when upper(regexp_replace("default_value", '::.*$', '')) in ('NOW()','CURRENT_TIMESTAMP') then ' DEFAULT CURRENT_TIMESTAMP'
	when upper(regexp_replace("default_value", '::.*$', '')) = 'CURRENT_DATE' then ' DEFAULT CURRENT_DATE'
	when upper(regexp_replace("default_value", '::.*$', '')) in ('TRUE','FALSE') then ' DEFAULT ' || upper(regexp_replace("default_value", '::.*$', ''))
	when "default_value" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}(::.*){0,1}$' then ' DEFAULT ' || regexp_replace("default_value", '::.*$', '')
	when "default_value" REGEXP_LIKE '^''.*''(::.*){0,1}$' then ' DEFAULT ' || regexp_replace("default_value", '^(''.*'')(::.*){0,1}$', '\1')
	else ''
end]]
coldef = [['"' || "exa_col" || '" ' || (]]..col_t..[[) || (]]..default_e..[[) || (case when "not_null" = 1 then ' NOT NULL' else '' end)]]

if decof == 'VARCHAR' then num_imp = [['cast(' || "col_q" || ' as varchar(128))']] else num_imp = [["col_q"]] end
-- BINARY/VARBINARY: the driver cannot transfer it raw, so read hex text via to_hex (HEX) or load NULL (SKIP).
if binmode == 'SKIP' then bin_src = [['cast(null as varchar(10))']] else bin_src = [['to_hex(' || "col_q" || ')']] end

-- source SELECT expression(s) for the IMPORT (must align positionally with coldef).
src = [[case
	when "base" = 'TIME' then 'cast(' || "col_q" || ' as varchar(15))'
	when "base" = 'TIME WITH TIME ZONE' then 'cast(' || "col_q" || ' as varchar(21))'
	when "base" = 'INTERVAL' then ]]..iv_src..[[
	when "base" = 'ST_GEOMETRY' then 'ST_ASTEXT(' || "col_q" || ')'
	when "base" = 'BINARY VARYING' then ]]..bin_src..[[
	when "base" = 'NUMERIC' then ]]..num_imp..[[
	else "col_q"
end]]

if cstate == 'FORCE_ENABLE' then sw = 'enable'; scomment = [[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; scomment = [[  -- matches Netezza source (keys active)]]
else sw = 'disable'; scomment = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]
fkname = [[coalesce(nullif("fk_name",''), "table_name" || '_FK_' || "ref_table")]]
known = [["base" in ('BYTEINT','SMALLINT','INTEGER','BIGINT','NUMERIC','REAL','DOUBLE','DOUBLE PRECISION','FLOAT','CHARACTER','CHARACTER VARYING','NATIONAL CHARACTER','NATIONAL CHARACTER VARYING','DATE','TIME','TIME WITH TIME ZONE','TIMESTAMP','INTERVAL','BOOLEAN','ST_GEOMETRY','BINARY VARYING','JSON','JSONB','JSONPATH')]]

-- optional CTEs --------------------------------------------------------------------------------------
comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(SCHEMA), trim(TABLENAME), 0, cast(null as varchar(128)), DESCRIPTION from _V_TABLE where OBJTYPE = ''TABLE'' and DESCRIPTION is not null]]..nzflt('SCHEMA','TABLENAME')..[[ union all select trim(SCHEMA), trim(NAME), ATTNUM, trim(ATTNAME), DESCRIPTION from _V_RELATION_COLUMN where TYPE = ''TABLE'' and DESCRIPTION is not null]]..nzflt('SCHEMA','NAME')..[[') c ("schema_name","table_name","sub","column_name","comment_text") )
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" = 0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" > 0)]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(SCHEMA), trim(VIEWNAME), CAST(DEFINITION AS VARCHAR(30000)) from _V_VIEW where 1 = 1]]..nzflt('SCHEMA','VIEWNAME')..[[') v ("schema_name","view_name","view_def") )
,vv_views as (select '-- ' || "schema_name" || '.' || "view_name" || '  (Netezza view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (Netezza SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

dist_cte = ''  dist_union = ''
if gen_dist then
	dist_cte = [[
,vv_dist_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(SCHEMA), trim(TABLENAME), trim(ATTNAME), DISTSEQNO from _V_TABLE_DIST_MAP where 1 = 1]]..nzflt('SCHEMA','TABLENAME')..[[') d ("schema_name","table_name","column_name","distseq"))
,vv_dist as (select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" DISTRIBUTE BY ' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "distseq") || ';' as sql_text
	from vv_dist_raw group by "schema_name","table_name")]]
	dist_union = "\n".. [[UNION ALL select 39, cast('-- ### DISTRIBUTE BY (from the Netezza hash distribution key) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 40, sql_text from vv_dist]]
end

-- CHECK_MIGRATION: per table a wide single-scan typed metrics row on BOTH systems; a per-schema summary
-- unpivots+joins them, flagging each metric OK/DEVIATION. Cross-comparable metrics: row/NULL/DISTINCT counts,
-- numeric MIN/MAX/SUM (exact integers/numeric only), date/timestamp MIN/MAX (text), variable-char length.
check_cte = ''  check_union = ''
if gen_check then
	chk_int  = [["base" in ('BYTEINT','SMALLINT','INTEGER','BIGINT')]]
	chk_dec  = [["base" = 'NUMERIC' and "p1" between 1 and 36]]
	chk_len  = [["base" in ('CHARACTER VARYING','NATIONAL CHARACTER VARYING')]]
	distinct_excl = [[ and "base" not in ('ST_GEOMETRY','REAL','DOUBLE','DOUBLE PRECISION','FLOAT','BINARY VARYING','JSON','JSONB','JSONPATH') ]]
	check_cte = [[
,vv_chk_base as (
	select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord", sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || x."exa_col" || '"' else x."col_q" end as "ref"
	from vv_columns x
	cross join (select 'Exasol' as "db_system" union all select 'Netezza' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 8) mid
)
,vv_chk_expr as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide_name",
	       (case
	          when "metric_id" = 0 and "ordinal_position" = "min_ord" then 'cast(count(*) as decimal(36,0))'
	          when "metric_id" = 1 and "not_null" = 0 then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))'
	          when "metric_id" = 2 and ((]]..chk_int..[[) or (]]..chk_dec..[[)) then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 2 and "base" = 'DATE' then (case when "db_system" = 'Exasol' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD'')' else 'to_char(min(' || "ref" || '),''YYYY-MM-DD'')' end)
	          when "metric_id" = 2 and "base" = 'TIMESTAMP' then (case when "db_system" = 'Exasol' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF6'')' else 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.US'')' end)
	          when "metric_id" = 3 and ((]]..chk_int..[[) or (]]..chk_dec..[[)) then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 3 and "base" = 'DATE' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD'')'
	          when "metric_id" = 3 and "base" = 'TIMESTAMP' then (case when "db_system" = 'Exasol' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF6'')' else 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.US'')' end)
	          when "metric_id" = 4 ]]..distinct_excl..[[ then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 5 and ((]]..chk_int..[[) or (]]..chk_dec..[[)) then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 6 and (]]..chk_len..[[) then 'cast(min(length(' || "ref" || ')) as decimal(36,0))'
	          when "metric_id" = 7 and (]]..chk_len..[[) then 'cast(max(length(' || "ref" || ')) as decimal(36,0))'
	        end) as "metric_expr"
	from vv_chk_base
)
,vv_chk_named as (
	select "exa_schema","exa_table","schema_name","table_name","ordinal_position","db_system","metric_id","wide_name","metric_expr",
	       (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_DISTINCT' when 5 then "exa_col" || '_SUM' when 6 then "exa_col" || '_MINLEN' when 7 then "exa_col" || '_MAXLEN' end) as "metric_name"
	from vv_chk_expr where "metric_expr" is not null
)
,vv_chk_sys as (
	select "exa_schema","exa_table","schema_name","table_name","wide_name","db_system",
	       'select ' || (case when "db_system" = 'Exasol' then 'cast(''Exasol'' as varchar(10)) as "DB_SYSTEM", ' else '''Netezza'' as db_system, ' end) || listagg("metric_expr" || (case when "db_system" = 'Exasol' then ' as "' || "metric_name" || '"' else '' end), ', ') within group (order by "ordinal_position","metric_id") || ' from ' || (case when "db_system" = 'Exasol' then '"' || "exa_schema" || '"."' || "exa_table" || '"' else '"' || "schema_name" || '"."' || "table_name" || '"' end) as "sys_select"
	from vv_chk_named group by "exa_schema","exa_table","schema_name","table_name","wide_name","db_system"
)
,vv_chk_wide as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "wide_name" || '" AS ' || max(case when "db_system" = 'Exasol' then "sys_select" end) || ' UNION ALL select * from (IMPORT FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || replace(max(case when "db_system" = 'Netezza' then "sys_select" end), '''', '''''') || '''' || ') ;' as sql_text
	from vv_chk_sys group by "exa_schema","wide_name"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system",
	       'select ' || '''' || "exa_table" || '''' || ' as "TABLE_NAME", ' || '''' || "metric_name" || '''' || ' as "METRIC", to_char("' || "metric_name" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide_name" || '" where "DB_SYSTEM" = ' || '''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'CREATE OR REPLACE TABLE "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" AS select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", t."VAL" as "NETEZZA_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(t."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || listagg(case when "db_system" = 'Exasol' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') e join (' || listagg(case when "db_system" = 'Netezza' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') t on e."TABLE_NAME" = t."TABLE_NAME" and e."METRIC" = t."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
	from vv_chk_unpiv group by "exa_schema"
)]]
	check_union = "\n".. [[UNION ALL select 70, cast('-- ### DATA VALIDATION (CHECK_MIGRATION) - run AFTER the IMPORTs; compares source vs target metrics ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 71, sql_text from vv_chk_wide
UNION ALL select 72, cast('-- per-schema validation summary (one row per metric; STATUS = OK / DEVIATION):' as varchar(2000000))
UNION ALL select 73, sql_text from vv_chk_summary
UNION ALL select 74, cast('-- review deviations with:  select * from "DATABASE_MIGRATION"."<schema>_MIG_CHK" where "STATUS" = ''DEVIATION'';' as varchar(2000000))]]
end

suc, res = pquery([[
with vv_columns as (
	select ]]..sname_e..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table", ]]..U('"column_name"')..[[ as "exa_col", '"' || "column_name" || '"' as "col_q",
	       case when "attnotnull" then 1 else 0 end as "not_null",
	       upper(trim(regexp_replace("ftype", '\(.*', ''))) as "base",
	       cast(regexp_substr("ftype", '[0-9]+', 1, 1) as decimal(18,0)) as "p1",
	       cast(regexp_substr("ftype", '[0-9]+', 1, 2) as decimal(18,0)) as "p2",
	       t.*
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..columns_q..[[') t ("schema_name","table_name","column_name","ordinal_position","ftype","attnotnull","default_value")
)
,vv_catchall as (
	select '-- NOTE: column "' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" has unmapped Netezza type ' || "ftype" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
	from vv_columns where not (]]..known..[[)
)
,vv_pk_raw as (select p.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..pk_q..[[') p ("schema_name","table_name","column_name","column_position") where exists (select 1 from vv_columns c where c."schema_name" = p."schema_name" and c."table_name" = p."table_name" and c."column_name" = p."column_name"))
,vv_pk as (
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" PRIMARY KEY (' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "column_position") || ') DISABLE;' as sql_text
	from vv_pk_raw group by "schema_name","table_name"
)
,vv_fk_raw as (select f.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..fk_q..[[') f ("schema_name","table_name","fk_name","fk_column","col_position","ref_schema","ref_table","ref_column") where exists (select 1 from vv_columns c where c."schema_name" = f."ref_schema" and c."table_name" = f."ref_table"))
,vv_fk as (
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U(fkname)..[[ || '" FOREIGN KEY (' || group_concat('"' || ]]..U('"fk_column"')..[[ || '"' order by "col_position") || ') REFERENCES "' || ]]..ref_sname_e..[[ || '"."' || ]]..U('"ref_table"')..[[ || '" (' || group_concat('"' || ]]..U('"ref_column"')..[[ || '"' order by "col_position") || ') DISABLE;' as sql_text
	from vv_fk_raw group by "schema_name","table_name","fk_name","ref_schema","ref_table"
)
,vv_create_schemas as (select distinct 'CREATE SCHEMA IF NOT EXISTS "' || "exa_schema" || '";' as sql_text from vv_columns)
,vv_create_tables as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "exa_table" || '" (' || group_concat((]]..coldef..[[) order by "ordinal_position" separator ', ') || ');' as sql_text
	from vv_columns group by "exa_schema","exa_table"
)
,vv_imports as (
	select 'IMPORT INTO "' || "exa_schema" || '"."' || "exa_table" || '" FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || 'select ' || group_concat((]]..src..[[) order by "ordinal_position" separator ', ') || ' from "' || "schema_name" || '"."' || "table_name" || '"' || '''' || ';' as sql_text
	from vv_columns group by "exa_schema","exa_table","schema_name","table_name"
)]]..comments_cte..views_cte..dist_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_catchall
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 3, cast('-- ### TABLES (incl. PRIMARY KEY, created DISABLED) ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk
	UNION ALL select 7, cast('-- ### FOREIGN KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 8, sql_text from vv_fk]]..comments_union..dist_union..[[
	UNION ALL select 50, cast('-- ### IMPORTS ###' as varchar(2000000))
	UNION ALL select 51, sql_text from vv_imports
	UNION ALL select 60, cast('-- ### CONSTRAINT STATE - run AFTER the data load (keys created DISABLED for a fast, order-independent load) ###' as varchar(2000000))
	UNION ALL select 61, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" ]]..sw..[[;]]..scomment..[[' from vv_pk_raw group by "schema_name","table_name"
	UNION ALL select 62, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U(fkname)..[[ || '" ]]..sw..[[;]]..scomment..[[' from vv_fk_raw group by "schema_name","table_name","fk_name","ref_table"]]..views_union..check_union..[[
) order by ord
]],{})

if not suc then error('"'..res.error_message..'" caught while executing: "'..res.statement_text..'"') end
return(res)
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Prerequisites
--   * The Netezza database must be reachable from this Exasol database.
--   * IMPORTANT: connect to the SOURCE database to migrate (…/MYDB), NOT to the SYSTEM database.
--
-- INSTALL THE NETEZZA JDBC DRIVER IN BUCKETFS  (Netezza-specific - do this BEFORE the CREATE CONNECTION below):
--   1. Get nzjdbc3.jar - it is NOT available on Maven and NOT publicly downloadable. Download it (a free IBM
--      registration is required) from IBM Fix Central ( https://www.ibm.com/support/fixcentral ): search for
--      "IBM Cloud Pak for Data System", select release NPS_11.3 and download. (The full direct link and the IBM
--      help pages are in the README Netezza section.)
--   2. Create a plain text file settings.cfg with EXACTLY this content:
--        DRIVERNAME=NETEZZA
--        DRIVERMAIN=org.netezza.Driver
--        PREFIX=jdbc:netezza:
--        NOSECURITY=YES
--        FETCHSIZE=100000
--        INSERTSIZE=-1
--   3. Upload BOTH nzjdbc3.jar AND settings.cfg into BucketFS (Exasol "add a JDBC driver"):
--        on-premise: https://docs.exasol.com/db/latest/administration/on-premise/manage_drivers/add_jdbc_driver.htm
--        SaaS:       https://docs.exasol.com/db/latest/administration/manage_drivers/add_jdbc_driver.htm
--      On-premise upload example (set WRITE_PW and DATABASE_NODE_IP to your values):
--        curl -k -X PUT -T settings.cfg https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/netezza/settings.cfg
--        curl -k -X PUT -T nzjdbc3.jar  https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/netezza/nzjdbc3.jar
--
-- Then create a connection to the Netezza source database (adjust host, database and credentials),
-- and run the accompanying test query.

CREATE OR REPLACE CONNECTION NETEZZA_JDBC
    TO 'jdbc:netezza://netezza_host:5480/my_source_database'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT NETEZZA_JDBC STATEMENT 'SELECT ''Connection works'' FROM _V_DUAL');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.NETEZZA_TO_EXASOL(
    'NETEZZA_JDBC',     -- CONNECTION_NAME: JDBC connection (pointing at the SOURCE database)
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'MYSCHEMA', 'APP_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'MY_TABLE', 'MY_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Netezza comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_DISTRIBUTION_BY: true (default) => add DISTRIBUTE BY from the Netezza hash distribution key; false => skip
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; numeric>36 -> DECIMAL(36,s); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
    'VARCHAR',          -- INTERVAL_HANDLING: 'VARCHAR' (recommended; interval as lossless text) or 'INTERVAL' (native Exasol INTERVAL, best-effort - day-time intervals only; year/month/sub-second NOT carried)
    'HEX',              -- BINARY_HANDLING: 'HEX' (recommended; BINARY/VARBINARY migrated losslessly as hex text via to_hex - max 32000 bytes) or 'SKIP' (load NULL)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
