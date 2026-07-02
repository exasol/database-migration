create schema if not exists database_migration;

/*
    oracle_to_exasol.sql  -  generate the statements to migrate an Oracle database to Exasol v8.

    Source: Oracle Database (verified on Oracle 26ai / 23.26). This script runs on the TARGET Exasol database, reads
    the SOURCE metadata through an Oracle connection - either an ORA (OCI / Oracle Instant Client, faster,
    recommended) or a JDBC connection; the connection type is AUTO-DETECTED - and RETURNS the statements (CREATE
    SCHEMA / CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / COMMENTs / IMPORT / a final CONSTRAINT STATE section /
    optional VIEW review section / optional DATA VALIDATION). It changes nothing itself - review the output and run
    it in the order returned. An Oracle schema (owner) maps to an Exasol schema.

    BOTH TRANSPORTS: every type is migrated so that it transfers correctly over BOTH "IMPORT FROM ORA" (OCI) and
    "IMPORT FROM JDBC". Oracle's non-scalar types are read through an Oracle-side conversion (TO_CHAR, RAWTOHEX,
    CAST, XMLSERIALIZE, JSON_SERIALIZE, FROM_VECTOR, SDO_UTIL.TO_WKTGEOMETRY, ...) so the transferred value is a
    plain VARCHAR2/NUMBER/TIMESTAMP that both transports handle identically.

    DATA TYPE MAPPING (every Oracle type CREATE-probed live on 26ai; transfer verified over BOTH ORA and JDBC):
      NUMBER(p,s) fixed -> DECIMAL(p,s) (p>36 -> DECIMAL_OVERFLOW; negative scale -> DECIMAL(p+|s|,0));
      NUMBER(p)/NUMBER(*,0)/INTEGER/INT/SMALLINT -> DECIMAL(min(p,36),0); NUMBER without precision -> DOUBLE (or
      VARCHAR via DECIMAL_OVERFLOW); FLOAT/BINARY_FLOAT/BINARY_DOUBLE -> DOUBLE (Inf/NaN -> NULL, Exasol has neither);
      CHAR/NCHAR -> CHAR UTF8 (>2000 -> VARCHAR); VARCHAR2/NVARCHAR2 -> VARCHAR UTF8; CLOB/NCLOB -> VARCHAR(2000000);
      RAW/BLOB -> VARCHAR hex (BINARY_HANDLING); DATE -> TIMESTAMP(0) (Oracle DATE carries a time component);
      TIMESTAMP(p) -> TIMESTAMP(min(p,9)); TIMESTAMP WITH [LOCAL] TIME ZONE -> TIMESTAMP (WITH TIME ZONE normalized to
      UTC); INTERVAL -> VARCHAR (INTERVAL_HANDLING, native Exasol INTERVAL optional/JDBC-only); XMLTYPE/JSON/VECTOR ->
      VARCHAR; SDO_GEOMETRY -> VARCHAR (WKT); BOOLEAN -> BOOLEAN. Anything else -> VARCHAR(2000000) catch-all (never
      silently dropped). Hard limits (fail loudly, never corrupt): NUMBER needing > 36 digits under
      DECIMAL_OVERFLOW='CAP'; a character/LOB value > 2,000,000 chars (unless TRUNCATE_LONG_STRINGS=true). Exasol's
      DATE/TIMESTAMP range is 0001-01-01 .. 9999-12-31 (no BC): a BC (year < 1) Oracle date is out of range - over the
      ORA transport the IMPORT fails loudly, so pre-process such values before migrating.

    OCI vs JDBC (verified): both transfer every type. Large CLOB/NCLOB stream to 2,000,000 chars only via JDBC; over
    OCI a LOB is read with TO_CHAR and is capped at 4000 chars (Oracle VARCHAR2 SQL limit; 32767 with EXTENDED
    MAX_STRING_SIZE). This matches Exasol's guidance to fall back to JDBC for CLOB/INTERVAL columns. The script emits
    the transport-matching read automatically. BLOB/RAW -> hex is capped at 2000 bytes/value (STANDARD MAX_STRING_SIZE).

    PARALLEL_STATEMENTS: if > 1, each table's IMPORT is split into that many parallel statements using balanced
    bin-packing of the table's partitions (from ALL_TAB_PARTITIONS); if a table has no partitions, ORA_HASH of the
    ROWID is used to split it into that many buckets.

    CONSTRAINTS: PRIMARY KEY / FOREIGN KEY are migrated, created DISABLED; a final CONSTRAINT STATE section sets them
    per CONSTRAINT_STATE (run after the IMPORTs). Identity/generated columns are migrated as plain columns.

    Not migrated (out of scope): indexes, UNIQUE/CHECK constraints, sequences, procedures/functions, triggers,
    synonyms, materialized-view logic; BLOB/RAW beyond the documented byte cap. Always excluded (only real user data):
    Oracle-maintained schemas (SYS, SYSTEM, XDB, MDSYS, ... via ALL_USERS.ORACLE_MAINTAINED='Y').
*/
--/
create or replace script database_migration.ORACLE_TO_EXASOL(
  CONNECTION_NAME               -- name of the Oracle connection inside Exasol (ORA/OCI or JDBC - auto-detected) -> e.g. ORACLE_OCI
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- source schema(s)/owner(s): 'MYSCHEMA', 'APP%', 'S1, S2', '%' (all user schemas; Oracle-maintained schemas always excluded)
  ,TABLE_FILTER                 -- table(s): 'MY_TABLE', 'MY%', 'T1, T2', '%' (all)
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema (owner) name
  ,PARALLEL_STATEMENTS          -- integer >= 1: number of parallel IMPORT statements per table (partition bin-packing if partitioned, else ORA_HASH(ROWID) buckets)
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate Oracle table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; NUMBER>36 -> DECIMAL(36,s), unscaled NUMBER -> DOUBLE), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
  ,BINARY_HANDLING              -- 'HEX' (recommended; RAW/BLOB migrated as hex text via RAWTOHEX; BLOB capped ~2000 bytes) or 'SKIP' (load NULL)
  ,INTERVAL_HANDLING            -- 'VARCHAR' (recommended; interval as lossless text, both transports) or 'INTERVAL' (native Exasol INTERVAL - JDBC connection only)
  ,TRUNCATE_LONG_STRINGS        -- true: char/LOB values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,CHECK_MIGRATION              -- true/false: additionally emit data-validation metrics (per-table "<table>_MIG_CHK" + a "<schema>_MIG_CHK" summary comparing source vs target). Run AFTER the IMPORTs.
) RETURNS TABLE
AS

-- ---- parameter handling -----------------------------------------------------------------------------
function startsWith(s, w) return string.sub(s, 1, string.len(w)) == w end
ps = 1
if type(PARALLEL_STATEMENTS) == 'number' then ps = math.floor(PARALLEL_STATEMENTS) end
if ps < 1 then ps = 1 end

exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end
function U(col) return exa_upper_begin..col..exa_upper_end end

cstate = string.upper(tostring(CONSTRAINT_STATE))
if cstate ~= 'SET_AS_SOURCE' and cstate ~= 'FORCE_ENABLE' then cstate = 'FORCE_DISABLE' end
gen_comments = (GENERATE_COMMENTS == true) or (string.upper(tostring(GENERATE_COMMENTS)) == 'TRUE')
gen_views    = (GENERATE_VIEWS == true) or (string.upper(tostring(GENERATE_VIEWS)) == 'TRUE')
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' and decof ~= 'VARCHAR' then decof = 'CAP' end
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'HEX' end
ivmode = string.upper(tostring(INTERVAL_HANDLING))
if ivmode ~= 'INTERVAL' then ivmode = 'VARCHAR' end
trunc    = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

-- ---- connection type auto-detection (ORA/OCI vs JDBC) -----------------------------------------------
function detect_conn(cn)
	local suc, res = pquery([[select CONNECTION_STRING from SYS.EXA_DBA_CONNECTIONS where CONNECTION_NAME = :c]], {c=cn})
	if suc and #res == 1 then
		if startsWith(string.upper(res[1][1]), 'JDBC') then return 'JDBC' else return 'ORA' end
	end
	-- fall back to trying each transport
	if pquery([[select * from (import from ora at ]]..cn..[[ statement 'select 1 from dual')]]) then return 'ORA' end
	if pquery([[select * from (import from jdbc at ]]..cn..[[ statement 'select 1 from dual')]]) then return 'JDBC' end
	error('Connection '..cn..' is neither a valid ORA nor JDBC connection.')
end
CT = detect_conn(CONNECTION_NAME)

-- ---- schema/table filter (comma list -> IN, wildcard -> LIKE); Oracle-maintained schemas excluded ---
function flt(val)
	if string.match(val, '%%') then
		return [[ like '']]..val..[['']]
	else
		return [[ in ('']]..val:gsub("^%s*(.-)%s*$","%1"):gsub('%s*,%s*',"'',''")..[['')]]
	end
end
SF = flt(SCHEMA_FILTER)
TF = flt(TABLE_FILTER)
-- only real user schemas: Oracle-maintained schemas (SYS, SYSTEM, XDB, MDSYS, CTXSYS, ...) carry ORACLE_MAINTAINED='Y';
-- PDBADMIN is a non-maintained but Oracle-created PDB admin account -> excluded explicitly.
usr_ok = [[owner in (select username from all_users where oracle_maintained = ''N'') and owner not in (''PDBADMIN'')]]

if TARGET_SCHEMA == null or TARGET_SCHEMA == '' then tschema = [["owner"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
if TARGET_SCHEMA == null or TARGET_SCHEMA == '' then ref_sname_e = U('"r_owner"') else ref_sname_e = sname_e end

-- ---- transport-aware source read snippets (CONNECTION_TYPE known at generation time) ----------------
-- CLOB: JDBC streams the raw LOB (up to 2,000,000); OCI must convert with TO_CHAR (capped at 4000 chars).
-- NCLOB: JDBC via TO_CLOB (stream); OCI via TO_CHAR. BOOLEAN: OCI accepts numeric 1/0, JDBC accepts ''true''/''false''.
if CT == 'JDBC' then
	clob_read  = [['"' || "column_name" || '"']]
	nclob_read = [['to_clob("' || "column_name" || '")']]
	bool_read  = [['case when "' || "column_name" || '" is null then null when "' || "column_name" || '" then ''true'' else ''false'' end']]
else
	clob_read  = [['to_char("' || "column_name" || '")']]
	nclob_read = [['to_char("' || "column_name" || '")']]
	bool_read  = [['case when "' || "column_name" || '" is null then null when "' || "column_name" || '" then 1 else 0 end']]
end
if trunc then
	clob_read  = [['substr(' || ]]..clob_read..[[ || ', 1, 2000000)']]
	nclob_read = [['substr(' || ]]..nclob_read..[[ || ', 1, 2000000)']]
end

-------------------------------------------------------------------------------------------------------
-- PARALLEL_STATEMENTS: balanced bin-packing of partitions (else ORA_HASH(ROWID)). Ported and kept.
-------------------------------------------------------------------------------------------------------
sql_ora_part_bin = [[select cast(null as varchar(128)) sn, cast(null as varchar(128)) tn, cast(null as varchar(128)) pn, cast(null as int) cnt, cast(null as int) bin_nr from dual]]
if ps > 1 then
	local suc, r1 = pquery([[
		select 'select ''''' || table_owner || ''''' SN, ''''' || table_name || ''''' TN, ''''' || partition_name || ''''' PN , count(*) cnt from "' || table_owner || '"."' || table_name || '" partition ("' || partition_name || '")'
		       || case when rownum != count(*) over() then ' union all ' end SQL_PART_CNT
		from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement
		      'select table_owner, table_name, partition_name from all_tab_partitions where table_owner in (select username from all_users where oracle_maintained = ''N'') and table_owner not in (''PDBADMIN'') and table_owner ]]..flt(SCHEMA_FILTER)..[[ and table_name ]]..TF..[[')
	]])
	if not suc then error(r1.error_message) end
	if #r1 >= 1 then
		local q2 = [[import into (SN varchar(128), TN varchar(128), PN varchar(128), CNT decimal(36,0)) from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement ']]
		for i=1,#r1 do q2 = q2 .. r1[i].SQL_PART_CNT end
		q2 = q2 .. [[']]
		local suc3, r3 = pquery([[select SN, TN, PN, CNT from (]]..q2..[[) where CNT > 0 order by SN, TN, CNT desc]])
		if not suc3 then error(r3.error_message) end
		local t_part = {}
		for i=1,#r3 do t_part[#t_part+1] = {SN=r3[i].SN, TN=r3[i].TN, PN=r3[i].PN, CNT=tonumber(r3[i].CNT)} end
		local s_sn, s_tn, t_bin, n_sum, vals = nil, nil, {}, 0, {}
		for i=1,#t_part do
			if s_sn == nil or t_part[i].SN ~= s_sn or s_tn == nil or t_part[i].TN ~= s_tn then
				s_sn = t_part[i].SN; s_tn = t_part[i].TN; t_bin = {}; n_sum = 0
			end
			local idx = 1
			for b=1,ps do
				if t_bin[b] == nil then idx = b; t_bin[b] = 0; break
				elseif t_bin[b] <= n_sum then idx = b; n_sum = t_bin[b] end
			end
			t_bin[idx] = t_bin[idx] + t_part[i].CNT
			n_sum = t_bin[idx]
			vals[#vals+1] = [[(']]..t_part[i].SN..[[', ']]..t_part[i].TN..[[', ']]..t_part[i].PN..[[', ]]..t_part[i].CNT..[[, ]]..idx..[[)]]
		end
		sql_ora_part_bin = [[select * from values ]]..table.concat(vals, ', ')..[[ as t(sn, tn, pn, cnt, bin_nr)]]
	end
end

-------------------------------------------------------------------------------------------------------
-- Metadata query (ALL_TAB_COLUMNS). Detect optional columns for portability across Oracle versions.
-------------------------------------------------------------------------------------------------------
cols_q = [[select owner, table_name, column_name, data_type, data_length, data_precision, data_scale, char_length, char_used, nullable, column_id
           from all_tab_columns c
           where ]]..usr_ok..[[ and owner ]]..SF..[[ and table_name ]]..TF..[[
           and (owner, table_name) in (select owner, table_name from all_tables where ]]..usr_ok..[[ and owner ]]..SF..[[ and table_name ]]..TF..[[)]]

pk_q = [[select acc.owner, acc.table_name, acc.column_name, acc.position pos
         from all_constraints ac join all_cons_columns acc on ac.owner=acc.owner and ac.constraint_name=acc.constraint_name and ac.table_name=acc.table_name
         where ac.constraint_type=''P'' and acc.owner ]]..SF..[[ and acc.table_name ]]..TF..[[]]

fk_q = [[select acc.owner, acc.table_name, acc.constraint_name, acc.column_name, acc.position pos,
                acc_r.owner r_owner, acc_r.table_name r_table_name, acc_r.column_name r_column_name
         from all_constraints ac
         join all_cons_columns acc on ac.owner=acc.owner and ac.constraint_name=acc.constraint_name and ac.table_name=acc.table_name
         join all_cons_columns acc_r on ac.r_owner=acc_r.owner and ac.r_constraint_name=acc_r.constraint_name and acc.position=acc_r.position
         where ac.constraint_type=''R'' and acc.owner ]]..SF..[[ and acc.table_name ]]..TF..[[]]

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions building the generated statement text.
-------------------------------------------------------------------------------------------------------
sc = [[(case when "data_scale" is null or "data_scale" < 0 then 0 when "data_scale" > 36 then 36 else "data_scale" end)]]
-- fixed NUMBER(p,s) target (with negative-scale and >36 handling), governed by DECIMAL_OVERFLOW
if decof == 'DOUBLE' then num_over = [['DOUBLE']] elseif decof == 'VARCHAR' then num_over = [['VARCHAR(50) ASCII']] else num_over = [['DECIMAL(36,' || ]]..sc..[[ || ')']] end
if decof == 'DOUBLE' then unscaled = [['DOUBLE']] elseif decof == 'VARCHAR' then unscaled = [['VARCHAR(50) ASCII']] else unscaled = [['DOUBLE']] end
num_fixed = [[case
	when "data_scale" < 0 then 'DECIMAL(' || (case when ("data_precision" - "data_scale") > 36 then 36 else ("data_precision" - "data_scale") end) || ',0)'
	when "data_precision" > 36 or "data_scale" > 36 then ]]..num_over..[[
	when "data_scale" > "data_precision" then 'DECIMAL(' || ]]..sc..[[ || ',' || ]]..sc..[[ || ')'
	else 'DECIMAL(' || "data_precision" || ',' || ]]..sc..[[ || ')'
end]]
-- integer-like NUMBER (scale 0, precision maybe null): cap at 36
num_int = [['DECIMAL(' || (case when "data_precision" is null or "data_precision" > 36 then 36 else "data_precision" end) || ',0)']]
if ivmode == 'INTERVAL' then
	iv_ym_t = [['INTERVAL YEAR(' || (case when "data_precision" is null or "data_precision"=0 then 2 else "data_precision" end) || ') TO MONTH']]
	iv_ds_t = [['INTERVAL DAY(' || (case when "data_precision" is null or "data_precision"=0 then 2 else "data_precision" end) || ') TO SECOND(' || (case when "data_scale" is null or "data_scale">9 then 9 else "data_scale" end) || ')']]
else
	iv_ym_t = [['VARCHAR(30) ASCII']]  iv_ds_t = [['VARCHAR(30) ASCII']]
end
if binmode == 'SKIP' then bin_t = [['VARCHAR(2000000) ASCII']] else bin_t = [['VARCHAR(' || (case when "data_length" is null or "data_length"*2 > 2000000 then 2000000 else "data_length"*2 end) || ') ASCII']] end

col_t = [[case
	when "dt" in ('CHAR','NCHAR') then case when "char_length" > 2000 then 'VARCHAR(' || "char_length" || ') UTF8' else 'CHAR(' || "char_length" || ') UTF8' end
	when "dt" in ('VARCHAR2','NVARCHAR2','VARCHAR') then 'VARCHAR(' || (case when "char_length" is null or "char_length" > 2000000 or "char_length"=0 then 2000000 else "char_length" end) || ') UTF8'
	when "dt" in ('CLOB','NCLOB','LONG') then 'VARCHAR(2000000) UTF8'
	when "dt" in ('XMLTYPE','JSON') then 'VARCHAR(2000000) UTF8'
	when "dt" = 'VECTOR' then 'VARCHAR(2000000) UTF8'
	when "dt" = 'SDO_GEOMETRY' then 'VARCHAR(2000000) UTF8'
	when "dt" in ('RAW','LONG RAW','BLOB') then ]]..bin_t..[[
	when "dt" = 'NUMBER' and "data_precision" is not null and "data_scale" is not null then ]]..num_fixed..[[
	when "dt" = 'NUMBER' and "data_scale" = 0 then ]]..num_int..[[
	when "dt" = 'NUMBER' then ]]..unscaled..[[
	when "dt" in ('FLOAT','BINARY_FLOAT','BINARY_DOUBLE') then 'DOUBLE'
	when "dt" = 'DATE' then 'TIMESTAMP(0)'
	when "dt" like 'TIMESTAMP%WITH%TIME ZONE' then 'TIMESTAMP(' || (case when "data_scale" is null or "data_scale">9 then 9 else "data_scale" end) || ')'
	when "dt" like 'TIMESTAMP%' then 'TIMESTAMP(' || (case when "data_scale" is null or "data_scale">9 then 9 else "data_scale" end) || ')'
	when "dt" like 'INTERVAL YEAR%' then ]]..iv_ym_t..[[
	when "dt" like 'INTERVAL DAY%' then ]]..iv_ds_t..[[
	when "dt" = 'BOOLEAN' then 'BOOLEAN'
	else 'VARCHAR(2000000) UTF8'
end]]

-- source read expression (aligns positionally with the CREATE TABLE column list)
if binmode == 'SKIP' then raw_read = [['cast(null as varchar2(10))']] else raw_read = [['rawtohex("' || "column_name" || '")']] end
if binmode == 'SKIP' then blob_read = [['cast(null as varchar2(10))']] else blob_read = [['rawtohex(dbms_lob.substr("' || "column_name" || '", 2000, 1))']] end
if trunc then vc_read = [['substr("' || "column_name" || '", 1, 2000000)']] else vc_read = [['"' || "column_name" || '"']] end
-- NUMBER that overflows into a VARCHAR target (DECIMAL_OVERFLOW='VARCHAR'): read as text, lossless and with the
-- decimal separator forced to '.' regardless of the Oracle session NLS (translate handles ','->'.'; raw NUMBER
-- would render as scientific notation over OCI). Numbers that map to DECIMAL/DOUBLE are read raw (typed = NLS-immune).
if decof == 'VARCHAR' then num_read_over = [['translate(to_char("' || "column_name" || '"), '','', ''.'')']] else num_read_over = [['"' || "column_name" || '"']] end
src = [[case
	when "dt" in ('VARCHAR2','NVARCHAR2','VARCHAR') then ]]..vc_read..[[
	when "dt" = 'CLOB' then ]]..clob_read..[[
	when "dt" = 'NCLOB' then ]]..nclob_read..[[
	when "dt" = 'LONG' then 'to_char("' || "column_name" || '")'
	when "dt" = 'RAW' then ]]..raw_read..[[
	when "dt" in ('BLOB','LONG RAW') then ]]..blob_read..[[
	when "dt" in ('FLOAT','BINARY_FLOAT','BINARY_DOUBLE') then 'case when "' || "column_name" || '" is infinite or "' || "column_name" || '" is nan then to_number(null) else cast("' || "column_name" || '" as number) end'
	when "dt" like 'TIMESTAMP%WITH LOCAL TIME ZONE' then 'cast("' || "column_name" || '" as timestamp)'
	when "dt" like 'TIMESTAMP%WITH TIME ZONE' then 'cast(sys_extract_utc("' || "column_name" || '") as timestamp)'
	when "dt" like 'INTERVAL %' then 'to_char("' || "column_name" || '")'
	when "dt" = 'XMLTYPE' then 'xmlserialize(content "' || "column_name" || '" as varchar2(4000))'
	when "dt" = 'JSON' then 'json_serialize("' || "column_name" || '" returning varchar2)'
	when "dt" = 'VECTOR' then 'from_vector("' || "column_name" || '" returning varchar2)'
	when "dt" = 'SDO_GEOMETRY' then 'to_char(sdo_util.to_wktgeometry("' || "column_name" || '"))'
	when "dt" = 'BOOLEAN' then ]]..bool_read..[[
	when "dt" = 'NUMBER' and (("data_precision" is null and "data_scale" is null) or ("data_precision" is not null and "data_scale" is not null and ("data_precision" > 36 or "data_scale" > 36))) then ]]..num_read_over..[[
	when "dt" in ('CHAR','NCHAR','NUMBER','DATE') then '"' || "column_name" || '"'
	when "dt" like 'TIMESTAMP%' then '"' || "column_name" || '"'
	else 'to_char("' || "column_name" || '")'
end]]

known = [["dt" in ('CHAR','NCHAR','VARCHAR2','NVARCHAR2','VARCHAR','CLOB','NCLOB','LONG','XMLTYPE','JSON','VECTOR','SDO_GEOMETRY','RAW','LONG RAW','BLOB','NUMBER','FLOAT','BINARY_FLOAT','BINARY_DOUBLE','DATE','BOOLEAN') or "dt" like 'TIMESTAMP%' or "dt" like 'INTERVAL %']]

if cstate == 'FORCE_ENABLE' then sw='enable'; scomment=[[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw='enable'; scomment=[[  -- matches Oracle source (keys active)]]
else sw='disable'; scomment=[[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]

-- ---- optional CTEs ----------------------------------------------------------------------------------
comments_cte='' comments_union=''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement 'select owner, table_name, 0 as sub, cast(null as varchar2(128)) as column_name, comments from all_tab_comments where comments is not null and ]]..usr_ok..[[ and owner ]]..SF..[[ and table_name ]]..TF..[[ union all select owner, table_name, 1 as sub, column_name, comments from all_col_comments where comments is not null and ]]..usr_ok..[[ and owner ]]..SF..[[ and table_name ]]..TF..[[') c ("owner","table_name","sub","column_name","comment_text"))
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub"=0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub">0)]]
	comments_union = "\n"..[[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte='' views_union=''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement 'select owner, view_name, text_vc from all_views where ]]..usr_ok..[[ and owner ]]..SF..[[ and view_name ]]..TF..[[') v ("owner","view_name","view_def"))
,vv_views as (select '-- ' || "owner" || '.' || "view_name" || '  - Oracle view, review and adapt to Exasol SQL manually' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n"..[[UNION ALL select 90, cast('-- ### VIEWS (Oracle SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

-- CHECK_MIGRATION: per table a wide typed-metrics row on BOTH systems; a per-schema summary flags OK/DEVIATION.
-- Mapping-aware: exact NUMBER(<=36) MIN/MAX/SUM, DATE/plain-TIMESTAMP MIN/MAX (to the second), NULL/DISTINCT counts;
-- excludes LOB/RAW/BLOB/XML/JSON/VECTOR/geo/INTERVAL/binary-float. NLS-safe: numbers typed, dates via numeric mask,
-- summary TO_CHAR's both sides on Exasol (so the result is consistent under any Exasol session NLS).
check_cte = ''  check_union = ''
if gen_check then
	chk_num = [["dt"='NUMBER' and "data_scale" is not null and "data_scale" between 0 and 36 and (case when "data_precision" is null then 36 else "data_precision" end) <= 36]]
	chk_dt  = [[("dt"='DATE' or ("dt" like 'TIMESTAMP%' and "dt" not like '%ZONE%'))]]
	dist_ok = [[("dt" in ('CHAR','NCHAR','VARCHAR2','NVARCHAR2','VARCHAR','BOOLEAN') or ]]..chk_dt..[[ or (]]..chk_num..[[))]]
	check_cte = [[
,vv_chk_cols as (select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord" from vv_columns x
	where (]]..known..[[) and "dt" not in ('CLOB','NCLOB','LONG','XMLTYPE','JSON','VECTOR','SDO_GEOMETRY','RAW','LONG RAW','BLOB','FLOAT','BINARY_FLOAT','BINARY_DOUBLE') and "dt" not like 'INTERVAL %')
,vv_chk_x as (
	select c.*, sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system"='Exasol' then '"' || c."exa_col" || '"' else '"' || c."column_name" || '"' end as "ref"
	from vv_chk_cols c
	cross join (select 'Exasol' as "db_system" union all select 'ORACLE' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 6) mid
)
,vv_chk_e as (
	select "exa_schema","exa_table","owner","table_name","exa_col","column_name","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide",
	   (case
	      when "metric_id"=0 and "ordinal_position"="min_ord" then (case when "db_system"='Exasol' then 'cast(count(*) as decimal(36,0))' else 'cast(count(*) as number)' end)
	      when "metric_id"=1 and "not_null"=0 then (case when "db_system"='Exasol' then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))' else 'cast(count(case when ' || "ref" || ' is null then 1 end) as number)' end)
	      when "metric_id"=2 and (]]..chk_num..[[) then (case when "db_system"='Exasol' then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'cast(min(' || "ref" || ') as number)' end)
	      when "metric_id"=2 and ]]..chk_dt..[[ then 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')'
	      when "metric_id"=3 and (]]..chk_num..[[) then (case when "db_system"='Exasol' then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'cast(max(' || "ref" || ') as number)' end)
	      when "metric_id"=3 and ]]..chk_dt..[[ then 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')'
	      when "metric_id"=4 and (]]..chk_num..[[) then (case when "db_system"='Exasol' then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'cast(sum(' || "ref" || ') as number)' end)
	      when "metric_id"=5 and ]]..dist_ok..[[ then (case when "db_system"='Exasol' then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))' else 'cast(count(distinct ' || "ref" || ') as number)' end)
	    end) as "mexpr",
	   (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_SUM' when 5 then "exa_col" || '_DISTINCT' end) as "mname"
	from vv_chk_x
)
,vv_chk_named as (select * from vv_chk_e where "mexpr" is not null)
,vv_chk_sys as (
	select "exa_schema","exa_table","owner","table_name","wide","db_system",
	   case when "db_system"='Exasol'
	     then 'select ''Exasol'' as "DB_SYSTEM", ' || group_concat("mexpr" || ' as "' || "mname" || '"' order by "ordinal_position","metric_id" separator ', ') || ' from "' || "exa_schema" || '"."' || "exa_table" || '"'
	     else 'select ''ORACLE'' as "DB_SYSTEM", x.* from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement ' || '''' || replace('select ' || group_concat("mexpr" order by "ordinal_position","metric_id" separator ', ') || ' from "' || "owner" || '"."' || "table_name" || '"', '''', '''''') || '''' || ') x'
	   end as "sel"
	from vv_chk_named group by "exa_schema","exa_table","owner","table_name","wide","db_system"
)
,vv_chk_create as (
	select 'create or replace table "' || "exa_schema" || '"."' || "wide" || '" as ' || "sel" || ';' as sql_text
	from vv_chk_sys where "db_system"='Exasol'
)
,vv_chk_insert as (
	select 'insert into "' || "exa_schema" || '"."' || "wide" || '" ' || "sel" || ';' as sql_text
	from vv_chk_sys where "db_system"='ORACLE'
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system","wide","mname",
	   'select ''' || "exa_table" || ''' as "TABLE_NAME", ''' || "mname" || ''' as "METRIC", to_char("' || "mname" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide" || '" where "DB_SYSTEM" = ''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'create or replace table "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" as select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", o."VAL" as "ORACLE_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(o."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || group_concat(case when "db_system"='Exasol' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') e join (' || group_concat(case when "db_system"='ORACLE' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') o on e."TABLE_NAME"=o."TABLE_NAME" and e."METRIC"=o."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
	from vv_chk_unpiv group by "exa_schema"
)]]
	check_union = "\n".. [[UNION ALL select 70, cast('-- ### DATA VALIDATION (CHECK_MIGRATION) - run AFTER the IMPORTs; compares source vs target metrics ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 71, sql_text from vv_chk_create
UNION ALL select 72, sql_text from vv_chk_insert
UNION ALL select 73, cast('-- per-schema validation summary - one row per metric, STATUS = OK / DEVIATION' as varchar(2000000))
UNION ALL select 74, sql_text from vv_chk_summary
UNION ALL select 75, cast('-- review deviations with:  select * from "DATABASE_MIGRATION"."<schema>_MIG_CHK" where "STATUS" = ''DEVIATION'';' as varchar(2000000))]]
end

suc, res = pquery([[
with vv_columns as (
	select ]]..sname_e..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table", ]]..U('"column_name"')..[[ as "exa_col",
	       upper(trim("data_type")) as "dt",
	       case when "nullable" = 'N' then 1 else 0 end as "not_null",
	       cast("data_length" as decimal(18,0)) as "data_length", cast("data_precision" as decimal(18,0)) as "data_precision",
	       cast("data_scale" as decimal(18,0)) as "data_scale", cast("char_length" as decimal(18,0)) as "char_length",
	       "owner","table_name","column_name","data_type", cast("column_id" as decimal(9,0)) as "ordinal_position"
	from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement ']]..cols_q..[[') t ("owner","table_name","column_name","data_type","data_length","data_precision","data_scale","char_length","char_used","nullable","column_id")
)
,vv_catchall as (
	select '-- NOTE: column "' || "owner" || '"."' || "table_name" || '"."' || "column_name" || '" has unmapped Oracle type ' || "data_type" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
	from vv_columns where not (]]..known..[[)
)
,vv_pk_raw as (select p.* from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement ']]..pk_q..[[') p ("owner","table_name","column_name","pos") where exists (select 1 from vv_columns c where c."owner"=p."owner" and c."table_name"=p."table_name" and c."column_name"=p."column_name"))
,vv_pk as (
	select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" ADD CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" PRIMARY KEY (' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "pos") || ') DISABLE;' as sql_text
	from vv_pk_raw group by "owner","table_name"
)
,vv_fk_raw as (select f.* from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement ']]..fk_q..[[') f ("owner","table_name","fk_name","column_name","pos","r_owner","r_table_name","r_column_name") where exists (select 1 from vv_columns c where c."owner"=f."r_owner" and c."table_name"=f."r_table_name"))
,vv_fk as (
	select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" ADD CONSTRAINT "' || ]]..U('"fk_name"')..[[ || '" FOREIGN KEY (' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "pos") || ') REFERENCES "' || ]]..ref_sname_e..[[ || '"."' || ]]..U('"r_table_name"')..[[ || '" (' || group_concat('"' || ]]..U('"r_column_name"')..[[ || '"' order by "pos") || ') DISABLE;' as sql_text
	from vv_fk_raw group by "owner","table_name","fk_name","r_owner","r_table_name"
)
,vv_create_schemas as (select distinct 'CREATE SCHEMA IF NOT EXISTS "' || "exa_schema" || '";' as sql_text from vv_columns)
,vv_create_tables as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "exa_table" || '" (' || group_concat('"' || "exa_col" || '" ' || (]]..col_t..[[) || (case when "not_null"=1 and (]]..known..[[) and "dt" not in ('CLOB','NCLOB','LONG','XMLTYPE','JSON','VECTOR','SDO_GEOMETRY','FLOAT','BINARY_FLOAT','BINARY_DOUBLE','RAW','LONG RAW','BLOB') then ' NOT NULL' else '' end) order by "ordinal_position" separator ', ') || ');' as sql_text
	from vv_columns group by "exa_schema","exa_table"
)
,vv_cl as (
	select "exa_schema","owner","exa_table","table_name",
	       group_concat('"' || "exa_col" || '"' order by "ordinal_position" separator ', ') as exa_col_list,
	       group_concat((]]..src..[[) order by "ordinal_position" separator ', ') as ora_col_list
	from vv_columns group by "exa_schema","owner","exa_table","table_name"
)
,vv_bin as (]]..sql_ora_part_bin..[[)
,vv_stmt_part as (
	select cl."exa_schema", cl."owner", cl."exa_table", cl."table_name", bp.bin_nr,
	       group_concat('select ' || cl.ora_col_list || ' from "' || cl."owner" || '"."' || cl."table_name" || '"' || case when bp.pn is not null then ' partition("' || bp.pn || '")' end separator ' union all ') stmt
	from vv_cl cl left join vv_bin bp on cl."owner"=bp.sn and cl."table_name"=bp.tn
	group by cl."exa_schema", cl."owner", cl."exa_table", cl."table_name", bp.bin_nr
)
,vv_stmt_oh as (
	select sp."exa_schema", sp."owner", sp."exa_table", sp."table_name",
	       case when sp.bin_nr is null and ]]..ps..[[ > 1 then sp.stmt || ' where ora_hash(rowid, ' || (]]..ps..[[ - 1) || ') = ' || oh.l else sp.stmt end stmt
	from vv_stmt_part sp left join (select level-1 l from dual connect by level <= ]]..ps..[[) oh on sp.bin_nr is null and ]]..ps..[[ > 1
)
,vv_imports as (
	select 'IMPORT INTO "' || cl."exa_schema" || '"."' || cl."exa_table" || '" (' || cl.exa_col_list || ') FROM ]]..CT..[[ AT ]]..CONNECTION_NAME..[[' || group_concat(' STATEMENT ''' || replace(o.stmt,'''','''''') || '''' separator '') || ';' as sql_text
	from vv_cl cl join vv_stmt_oh o on cl."owner"=o."owner" and cl."table_name"=o."table_name"
	group by cl."exa_schema",cl."exa_table",cl.exa_col_list
)
,vv_nls as (select * from (import from ]]..CT..[[ at ]]..CONNECTION_NAME..[[ statement 'select parameter, value from nls_database_parameters where parameter in (''NLS_CHARACTERSET'',''NLS_NCHAR_CHARACTERSET'',''NLS_NUMERIC_CHARACTERS'',''NLS_DATE_FORMAT'')') n ("parameter","value"))]]..comments_cte..views_cte..check_cte..[[
select sql_text from (
	select -3 ord, cast('-- ### NLS / ENCODING (informational - this migration is NLS-independent) ###' as varchar(2000000)) SQL_TEXT
	UNION ALL select -2, '-- source Oracle ' || "parameter" || ' = ' || "value" from vv_nls
	UNION ALL select -1, cast('-- character data -> Exasol UTF8; NUMBER/DATE/TIMESTAMP transferred TYPED (NLS-immune); number/interval text normalized to ''.'' decimal separator (translate/to_char).' as varchar(2000000))
	UNION ALL select 0, sql_text from vv_catchall
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 3, cast('-- ### TABLES ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk
	UNION ALL select 7, cast('-- ### FOREIGN KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 8, sql_text from vv_fk]]..comments_union..[[
	UNION ALL select 50, cast('-- ### IMPORTS ###' as varchar(2000000))
	UNION ALL select 51, sql_text from vv_imports
	UNION ALL select 60, cast('-- ### CONSTRAINT STATE - run AFTER the data load ###' as varchar(2000000))
	UNION ALL select 61, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" ]]..sw..[[;]]..scomment..[[' from vv_pk_raw group by "owner","table_name"
	UNION ALL select 62, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U('"fk_name"')..[[ || '" ]]..sw..[[;]]..scomment..[[' from vv_fk_raw group by "owner","table_name","fk_name"]]..views_union..check_union..[[
) order by ord
]],{})

if not suc then error('"'..res.error_message..'" caught while executing: "'..res.statement_text..'"') end
return(res)
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Exasol recommends the ORA (OCI) connection with the Oracle Instant Client - it is the fastest way to migrate
-- from Oracle. A JDBC connection also works (and is the documented fallback for large CLOB / INTERVAL columns);
-- this script auto-detects which one CONNECTION_NAME is.
--
-- ORACLE INSTANT CLIENT (for the ORA/OCI connection) - VERSION COMPATIBILITY MATTERS:
--   The required Instant Client version depends on your EXASOL version (see the table in Exasol's docs):
--     https://docs.exasol.com/db/latest/administration/on-premise/manage_drivers/oracle_instant_client.htm
--     Exasol <= 8.31.0            -> instantclient 12.1.0.2.0
--     Exasol 8.32.0 .. 2025.1.8   -> instantclient 23.5.0.24.07
--     Exasol 2025.1.9 and higher  -> instantclient-basic-linux.x64-23.9.0.25.07.zip
--   Upload the matching Instant Client zip to BucketFS.
--
-- ORACLE JDBC DRIVER (for the JDBC connection): download the latest driver ojdbc11 from Maven
--   (https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc11) and, with a settings.cfg,
--   upload both to BucketFS as described here:
--     https://docs.exasol.com/db/latest/loading_data/connect_sources/oracle.htm#OracleJDBC
--
-- Oracle to Exasol migration guide: https://docs.exasol.com/db/latest/migration_guides/oracle/oracle_exasol.htm
--
--
-- Create a connection to the Oracle database (adjust host, database name and credentials),
-- then run the accompanying test query.
--
-- ORA (OCI) connection (fast, recommended):
CREATE OR REPLACE CONNECTION ORACLE_OCI
    TO 'oracle_host:1521/oracle_db_service'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM ORA AT ORACLE_OCI STATEMENT 'select ''Connection works'' from dual');

-- JDBC connection (fallback, e.g. for large CLOB / INTERVAL columns):
CREATE OR REPLACE CONNECTION ORACLE_JDBC
    TO 'jdbc:oracle:thin:@//oracle_host:1521/oracle_db_service'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT ORACLE_JDBC STATEMENT 'select ''Connection works'' from dual');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.ORACLE_TO_EXASOL(
    'ORACLE_OCI',       -- CONNECTION_NAME: Oracle connection (ORA/OCI or JDBC - auto-detected)
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    'MYSCHEMA',         -- SCHEMA_FILTER: source schema(s)/owner(s): 'MYSCHEMA', 'APP%', 'S1, S2', '%' (all; Oracle-maintained schemas always excluded)
    '%',                -- TABLE_FILTER: table(s): 'MY_TABLE', 'MY%', 'T1, T2', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    4,                  -- PARALLEL_STATEMENTS: 1 = one IMPORT per table; N>1 = N parallel statements (partition bin-packing else ORA_HASH(ROWID))
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Oracle comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; NUMBER>36 -> DECIMAL(36,s), unscaled NUMBER -> DOUBLE), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
    'HEX',              -- BINARY_HANDLING: 'HEX' (recommended; RAW/BLOB as hex text; BLOB capped ~2000 bytes) or 'SKIP' (load NULL)
    'VARCHAR',          -- INTERVAL_HANDLING: 'VARCHAR' (recommended; lossless text, both transports) or 'INTERVAL' (native Exasol INTERVAL - JDBC connection only)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
