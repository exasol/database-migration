create schema if not exists database_migration;

/*
    saphana_to_exasol.sql  -  generate the statements to migrate an SAP HANA database to Exasol v8.

    Source: SAP HANA 2.0 (verified on HANA 2.0 SPS08). This script runs on the TARGET Exasol database, reads the
    SOURCE metadata through a JDBC connection (native SYS.* catalog) and RETURNS the statements (CREATE SCHEMA /
    CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / COMMENTs / IMPORT / a final CONSTRAINT STATE section / optional
    VIEW review section / optional DATA VALIDATION). It changes nothing itself - review the output and run it in
    the order returned.

    DATA TYPE MAPPING (every SAP HANA type is covered; each CREATE-probed live on HANA 2.0 SPS08):
      TINYINT (unsigned 0-255) -> DECIMAL(3,0); SMALLINT/INTEGER/BIGINT -> DECIMAL(5/10/19,0);
      DECIMAL(p,s) (fixed) -> DECIMAL(p,s) (p>36 -> DECIMAL_OVERFLOW); DECIMAL without scale and SMALLDECIMAL are
      FLOATING-point decimals -> DOUBLE or VARCHAR (DECIMAL_OVERFLOW); REAL/DOUBLE/FLOAT -> DOUBLE;
      CHAR/NCHAR -> CHAR UTF8 (>2000 -> VARCHAR); VARCHAR/NVARCHAR/ALPHANUM/SHORTTEXT -> VARCHAR UTF8;
      CLOB/NCLOB/TEXT/BINTEXT -> VARCHAR(2000000); DATE -> DATE; SECONDDATE -> TIMESTAMP(0);
      TIMESTAMP -> TIMESTAMP(7) (HANA keeps 7 fractional digits); BOOLEAN -> BOOLEAN; ST_POINT/ST_GEOMETRY ->
      GEOMETRY (WKT). Small documented difference: TIME -> VARCHAR(8) (Exasol has no TIME type);
      BINARY/VARBINARY/BLOB -> VARCHAR hex (BINARY_HANDLING). Anything unexpected -> VARCHAR(2000000) catch-all.
      Hard limits (fail loudly, never corrupt): a fixed DECIMAL needing > 36 digits under DECIMAL_OVERFLOW='CAP';
      a binary value whose hex would exceed 2,000,000 chars (> ~1,000,000 bytes) under BINARY_HANDLING='HEX';
      a character/LOB value > 2,000,000 chars (unless TRUNCATE_LONG_STRINGS=true).

    WHY SOME COLUMNS ARE READ WITH A FUNCTION ON THE SOURCE (verified live with the SAP HANA JDBC driver, ngdbc):
    the driver cannot transfer some types directly, so the generated IMPORT reads them as text -
      BINARY/VARBINARY/BLOB ("JDBC type unknown") -> BINTOHEX(..);  ST_POINT/ST_GEOMETRY -> "col".ST_AsText() (WKT);
      NCLOB/TEXT/BINTEXT -> TO_NVARCHAR(..);  TIME (transfers as a TIMESTAMP with TODAY's date!) -> TO_VARCHAR(..)
      -> HH:MI:SS;  floating DECIMAL/SMALLDECIMAL in VARCHAR mode -> TO_VARCHAR(..). Everything else (integers,
      fixed DECIMAL, REAL/DOUBLE, CHAR/VARCHAR/NCHAR/NVARCHAR/ALPHANUM/SHORTTEXT/CLOB, DATE, SECONDDATE, TIMESTAMP
      with full microseconds, BOOLEAN) transfers directly.

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are not
    affected by differing source/target locale settings. Character data is stored as UTF8.

    CONSTRAINTS: PRIMARY KEY / FOREIGN KEY are migrated, created DISABLED; a final CONSTRAINT STATE section sets
    them per CONSTRAINT_STATE (run after the IMPORTs). Identity and generated columns are migrated as plain columns
    carrying their values (Exasol has no IDENTITY/computed columns).

    Not migrated (out of scope): indexes, UNIQUE/CHECK constraints, sequences, procedures/functions, synonyms,
    triggers, and HANA partitioning (physical/distribution-oriented; no value-based Exasol equivalent).

    Always excluded (only real user data): the SAP HANA system schemas (SYS, _SYS_*, SAP_*, HANA_*, PUBLIC, UIS);
    the DBA schema SYSTEM is excluded unless INCLUDE_SYSTEM_SCHEMA = true.
*/
--/
create or replace script database_migration.SAPHANA_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol (to the SAP HANA source) -> e.g. SAPHANA_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended; SAP HANA folds unquoted names to UPPER) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- filter for the source schemas (system schemas always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate SAP HANA table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; fixed DECIMAL>36 -> DECIMAL(36,s), floating DECIMAL/SMALLDECIMAL -> DOUBLE), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
  ,BINARY_HANDLING             -- 'HEX' (recommended; BINARY/VARBINARY/BLOB migrated losslessly as hex text via BINTOHEX) or 'SKIP' (load NULL)
  ,TRUNCATE_LONG_STRINGS        -- true: char/LOB values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,INCLUDE_SYSTEM_SCHEMA        -- true/false: also migrate the SYSTEM (DBA) schema; false (recommended) => exclude it (SYS/_SYS_*/SAP_*/HANA_*/PUBLIC are always excluded)
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
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' and decof ~= 'VARCHAR' then decof = 'CAP' end
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'HEX' end
trunc    = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
incl_system = (INCLUDE_SYSTEM_SCHEMA == true) or (string.upper(tostring(INCLUDE_SYSTEM_SCHEMA)) == 'TRUE')
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
if TARGET_SCHEMA == null then tschema = [["schema_name"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
if TARGET_SCHEMA == null then ref_sname_e = U('"ref_schema"') else ref_sname_e = sname_e end

-- SAP HANA filter (system schemas always excluded; SYSTEM gated by INCLUDE_SYSTEM_SCHEMA). LEFT(..) avoids a
-- LIKE-ESCAPE clause so no backslash-before-quote (keeps the script DbVisualizer-clean). One quoting level here.
function hflt(s, tn)
	local f = [[ and left(]]..s..[[,5) <> ''_SYS_'' and left(]]..s..[[,4) <> ''SAP_'' and left(]]..s..[[,5) <> ''HANA_'' and ]]..s..[[ not in (''SYS'',''PUBLIC'',''UIS'',''_SYS_BIC'')]]
	if not incl_system then f = f..[[ and ]]..s..[[ <> ''SYSTEM'']] end
	return f..[[ and ]]..s..[[ like '']]..SCHEMA_FILTER..[['' and ]]..tn..[[ like '']]..TABLE_FILTER..[['' ]]
end

-------------------------------------------------------------------------------------------------------
-- Remote (SAP HANA) metadata queries (native SYS.* catalog). Inner literals are quote-doubled.
-------------------------------------------------------------------------------------------------------
columns_q = [[select SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, POSITION, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE, DEFAULT_VALUE from SYS.TABLE_COLUMNS where IS_HIDDEN = ''FALSE'']]..hflt('SCHEMA_NAME','TABLE_NAME')

pk_q = [[select c.SCHEMA_NAME, c.TABLE_NAME, c.COLUMN_NAME, c.POSITION from SYS.CONSTRAINTS c where c.IS_PRIMARY_KEY = ''TRUE'']]..hflt('c.SCHEMA_NAME','c.TABLE_NAME')

fk_q = [[select SCHEMA_NAME, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, POSITION, REFERENCED_SCHEMA_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME from SYS.REFERENTIAL_CONSTRAINTS where 1=1]]..hflt('SCHEMA_NAME','TABLE_NAME')

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions producing the generated statement text.
-------------------------------------------------------------------------------------------------------
sc = [[(case when "scale" is null or "scale" < 0 then 0 when "scale" > 36 then 36 else "scale" end)]]
if decof == 'VARCHAR' then float_dec_t = [['VARCHAR(50) ASCII']] else float_dec_t = [['DOUBLE']] end
if decof == 'DOUBLE' then
	dec_fixed = [[case when "len" > 36 then 'DOUBLE' else 'DECIMAL(' || "len" || ',' || ]]..sc..[[ || ')' end]]
elseif decof == 'VARCHAR' then
	dec_fixed = [[case when "len" > 36 then 'VARCHAR(50) ASCII' else 'DECIMAL(' || "len" || ',' || ]]..sc..[[ || ')' end]]
else
	dec_fixed = [[case when "len" > 36 then 'DECIMAL(36,' || ]]..sc..[[ || ')' else 'DECIMAL(' || "len" || ',' || ]]..sc..[[ || ')' end]]
end
-- binary target (HEX -> 2x length hex text capped at 2,000,000; SKIP -> VARCHAR placeholder, NULL loaded)
if binmode == 'SKIP' then
	bin_t = [['VARCHAR(2000000) ASCII']]
else
	bin_t = [['VARCHAR(' || (case when "len" is null or "len" * 2 > 2000000 then 2000000 else "len" * 2 end) || ') ASCII']]
end

col_t = [[case
	when "dtype" = 'TINYINT' then 'DECIMAL(3,0)'
	when "dtype" = 'SMALLINT' then 'DECIMAL(5,0)'
	when "dtype" = 'INTEGER' then 'DECIMAL(10,0)'
	when "dtype" = 'BIGINT' then 'DECIMAL(19,0)'
	when "dtype" = 'DECIMAL' then case when "scale" is null then ]]..float_dec_t..[[ else ]]..dec_fixed..[[ end
	when "dtype" = 'SMALLDECIMAL' then ]]..float_dec_t..[[
	when "dtype" in ('REAL','DOUBLE','FLOAT') then 'DOUBLE'
	when "dtype" in ('CHAR','NCHAR') then case when "len" > 2000 then 'VARCHAR(' || "len" || ') UTF8' else 'CHAR(' || "len" || ') UTF8' end
	when "dtype" in ('VARCHAR','NVARCHAR','ALPHANUM','SHORTTEXT') then 'VARCHAR(' || (case when "len" is null or "len" > 2000000 then 2000000 else "len" end) || ') UTF8'
	when "dtype" in ('CLOB','NCLOB','TEXT','BINTEXT') then 'VARCHAR(2000000) UTF8'
	when "dtype" in ('VARBINARY','BINARY','BLOB') then ]]..bin_t..[[
	when "dtype" = 'DATE' then 'DATE'
	when "dtype" = 'TIME' then 'VARCHAR(8) ASCII'
	when "dtype" = 'SECONDDATE' then 'TIMESTAMP(0)'
	when "dtype" = 'TIMESTAMP' then 'TIMESTAMP(7)'
	when "dtype" = 'BOOLEAN' then 'BOOLEAN'
	when "dtype" in ('ST_POINT','ST_GEOMETRY') then 'GEOMETRY'
	else 'VARCHAR(2000000) UTF8'
end]]

-- DEFAULT mapping (literals + the common "now"; sequences/identity skipped).
default_e = [[case
	when "default_value" is null then ''
	when upper("default_value") in ('CURRENT_TIMESTAMP','NOW()','CURRENT_UTCTIMESTAMP') then ' DEFAULT CURRENT_TIMESTAMP'
	when upper("default_value") = 'CURRENT_DATE' then ' DEFAULT CURRENT_DATE'
	when upper("default_value") in ('TRUE','FALSE') then ' DEFAULT ' || upper("default_value")
	when "default_value" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}$' then ' DEFAULT ' || "default_value"
	when "default_value" REGEXP_LIKE '^''.*''$' then ' DEFAULT ' || "default_value"
	else ''
end]]
coldef = [['"' || "exa_col" || '" ' || (]]..col_t..[[) || (]]..default_e..[[) || (case when "not_null" = 1 then ' NOT NULL' else '' end)]]

-- source SELECT expression(s) for the IMPORT (must align positionally with coldef).
if binmode == 'SKIP' then bin_src = [['cast(null as varchar(10))']] else bin_src = [['BINTOHEX("' || "column_name" || '")']] end
if trunc then lob_src = [['LEFT(TO_NVARCHAR("' || "column_name" || '"), 2000000)']] else lob_src = [['TO_NVARCHAR("' || "column_name" || '")']] end
if trunc then txt_src = [['LEFT("' || "column_name" || '", 2000000)']] else txt_src = [['"' || "column_name" || '"']] end
src = [[case
	when "dtype" = 'TIME' then 'TO_VARCHAR("' || "column_name" || '")'
	when "dtype" in ('VARBINARY','BINARY','BLOB') then ]]..bin_src..[[
	when "dtype" in ('CLOB','NCLOB','TEXT','BINTEXT') then ]]..lob_src..[[
	when "dtype" in ('ST_POINT','ST_GEOMETRY') then '"' || "column_name" || '".ST_AsText()'
	when "dtype" in ('DECIMAL','SMALLDECIMAL') and ']]..decof..[[' = 'VARCHAR' then 'TO_VARCHAR("' || "column_name" || '")'
	when "dtype" in ('VARCHAR','NVARCHAR','ALPHANUM','SHORTTEXT') then ]]..txt_src..[[
	else '"' || "column_name" || '"'
end]]

if cstate == 'FORCE_ENABLE' then sw = 'enable'; scomment = [[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; scomment = [[  -- matches SAP HANA source (keys active)]]
else sw = 'disable'; scomment = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]
fkname = [[coalesce(nullif("fk_name",''), "table_name" || '_FK_' || "ref_table")]]
known = [["dtype" in ('TINYINT','SMALLINT','INTEGER','BIGINT','DECIMAL','SMALLDECIMAL','REAL','DOUBLE','FLOAT','CHAR','NCHAR','VARCHAR','NVARCHAR','ALPHANUM','SHORTTEXT','CLOB','NCLOB','TEXT','BINTEXT','VARBINARY','BINARY','BLOB','DATE','TIME','SECONDDATE','TIMESTAMP','BOOLEAN','ST_POINT','ST_GEOMETRY')]]

-- optional CTEs --------------------------------------------------------------------------------------
comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select SCHEMA_NAME, TABLE_NAME, 0 as SUB, cast(null as nvarchar(256)) as COLUMN_NAME, COMMENTS from SYS.TABLES where COMMENTS is not null]]..hflt('SCHEMA_NAME','TABLE_NAME')..[[ union all select SCHEMA_NAME, TABLE_NAME, POSITION as SUB, COLUMN_NAME, COMMENTS from SYS.TABLE_COLUMNS where COMMENTS is not null]]..hflt('SCHEMA_NAME','TABLE_NAME')..[[') c ("schema_name","table_name","sub","column_name","comment_text"))
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" = 0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" > 0)]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select SCHEMA_NAME, VIEW_NAME, CAST(DEFINITION AS NVARCHAR(5000)) from SYS.VIEWS where DEFINITION is not null]]..hflt('SCHEMA_NAME','VIEW_NAME')..[[') v ("schema_name","view_name","view_def"))
,vv_views as (select '-- ' || "schema_name" || '.' || "view_name" || '  (SAP HANA view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (SAP HANA SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

-- CHECK_MIGRATION: per table a wide single-scan typed metrics row on BOTH systems; a per-schema summary unpivots+
-- joins them, flagging each metric OK/DEVIATION. Mapping-aware: exact integer/fixed-decimal MIN/MAX/SUM only
-- (NOT floating DECIMAL/SMALLDECIMAL/REAL/DOUBLE), date/timestamp MIN/MAX as text to the second, no binary/LOB/
-- spatial value metrics. Source metric SELECT embedded via IMPORT (quotes doubled by REPLACE).
check_cte = ''  check_union = ''
if gen_check then
	chk_num  = [["dtype" in ('TINYINT','SMALLINT','INTEGER','BIGINT') or ("dtype" = 'DECIMAL' and "scale" is not null and "len" between 1 and 36)]]
	chk_dt   = [["dtype" in ('DATE','SECONDDATE','TIMESTAMP')]]
	dist_ok  = [["dtype" in ('TINYINT','SMALLINT','INTEGER','BIGINT','CHAR','NCHAR','VARCHAR','NVARCHAR','ALPHANUM','SHORTTEXT','DATE','SECONDDATE','TIMESTAMP') or ("dtype" = 'DECIMAL' and "scale" is not null)]]
	check_cte = [[
,vv_chk_cols as (select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord" from vv_columns x where ]]..known..[[)
,vv_chk_x as (
	select c.*, sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || c."exa_col" || '"' else '"' || c."column_name" || '"' end as "ref"
	from vv_chk_cols c
	cross join (select 'Exasol' as "db_system" union all select 'SAPHANA' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 6) mid
)
,vv_chk_e as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","column_name","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide",
	   (case
	      when "metric_id" = 0 and "ordinal_position" = "min_ord" then 'cast(count(*) as decimal(36,0))'
	      when "metric_id" = 1 and "not_null" = 0 then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))'
	      when "metric_id" = 2 and (]]..chk_num..[[) then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	      when "metric_id" = 2 and (]]..chk_dt..[[) then (case when "db_system" = 'Exasol' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')' else 'to_varchar(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')' end)
	      when "metric_id" = 3 and (]]..chk_num..[[) then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	      when "metric_id" = 3 and (]]..chk_dt..[[) then (case when "db_system" = 'Exasol' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')' else 'to_varchar(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')' end)
	      when "metric_id" = 4 and (]]..chk_num..[[) then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	      when "metric_id" = 5 and (]]..dist_ok..[[) then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))'
	    end) as "mexpr",
	   (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_SUM' when 5 then "exa_col" || '_DISTINCT' end) as "mname"
	from vv_chk_x
)
,vv_chk_named as (select * from vv_chk_e where "mexpr" is not null)
,vv_chk_sys as (
	select "exa_schema","exa_table","schema_name","table_name","wide","db_system",
	   case when "db_system" = 'Exasol'
	     then 'select ''Exasol'' as "DB_SYSTEM", ' || group_concat("mexpr" || ' as "' || "mname" || '"' order by "ordinal_position","metric_id" separator ', ') || ' from "' || "exa_schema" || '"."' || "exa_table" || '"'
	     else 'select ''SAPHANA'' as "DB_SYSTEM", x.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ' || '''' || replace('select ' || group_concat("mexpr" order by "ordinal_position","metric_id" separator ', ') || ' from "' || "schema_name" || '"."' || "table_name" || '"', '''', '''''') || '''' || ') x'
	   end as "sel"
	from vv_chk_named group by "exa_schema","exa_table","schema_name","table_name","wide","db_system"
)
,vv_chk_wide as (
	select 'create or replace table "' || "exa_schema" || '"."' || "wide" || '" as ' || max(case when "db_system" = 'Exasol' then "sel" end) || ' UNION ALL ' || max(case when "db_system" = 'SAPHANA' then "sel" end) || ';' as sql_text
	from vv_chk_sys group by "exa_schema","wide"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system","wide","mname",
	   'select ''' || "exa_table" || ''' as "TABLE_NAME", ''' || "mname" || ''' as "METRIC", to_char("' || "mname" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide" || '" where "DB_SYSTEM" = ''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'create or replace table "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" as select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", h."VAL" as "SAPHANA_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(h."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || group_concat(case when "db_system" = 'Exasol' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') e join (' || group_concat(case when "db_system" = 'SAPHANA' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') h on e."TABLE_NAME" = h."TABLE_NAME" and e."METRIC" = h."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
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
	select ]]..sname_e..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table", ]]..U('"column_name"')..[[ as "exa_col",
	       case when "is_nullable" = 'TRUE' then 0 else 1 end as "not_null",
	       upper(trim("data_type")) as "dtype",
	       cast("length" as decimal(18,0)) as "len", cast("scale" as decimal(18,0)) as "scale",
	       "schema_name", "table_name", "column_name", "ordinal_position", "data_type", "default_value"
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..columns_q..[[') t ("schema_name","table_name","column_name","ordinal_position","data_type","length","scale","is_nullable","default_value")
)
,vv_catchall as (
	select '-- NOTE: column "' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" has unmapped SAP HANA type ' || "data_type" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
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
)]]..comments_cte..views_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_catchall
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
--   * The SAP HANA database must be reachable from this Exasol database.
--
-- INSTALL THE SAP HANA JDBC DRIVER IN BUCKETFS  (do this BEFORE the CREATE CONNECTION below):
--   1. Download the SAP HANA JDBC driver ngdbc (ngdbc-2.x.jar) from Maven:
--        https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc
--      SAP HANA JDBC driver documentation:
--        https://help.sap.com/docs/SAP_HANA_CLIENT/f1b440ded6144a54ada97ff95dac7adf/ff15928cf5594d78b841fbbe649f04b4.html
--   2. Create a plain text file settings.cfg with EXACTLY this content:
--        DRIVERNAME=SAPHANA
--        DRIVERMAIN=com.sap.db.jdbc.Driver
--        PREFIX=jdbc:sap:
--        NOSECURITY=YES
--        FETCHSIZE=100000
--        INSERTSIZE=-1
--   3. Upload BOTH ngdbc-2.x.jar AND settings.cfg into BucketFS (Exasol "add a JDBC driver"):
--        on-premise: https://docs.exasol.com/db/latest/administration/on-premise/manage_drivers/add_jdbc_driver.htm
--        SaaS:       https://docs.exasol.com/db/latest/administration/manage_drivers/add_jdbc_driver.htm
--      On-premise upload example (set WRITE_PW and DATABASE_NODE_IP to your values):
--        curl -k -X PUT -T settings.cfg     https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/saphana/settings.cfg
--        curl -k -X PUT -T ngdbc-2.29.7.jar https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/saphana/ngdbc-2.29.7.jar
--
-- Then create a connection to the SAP HANA source database (adjust host, port and credentials), and run the
-- accompanying test query. The default SAP HANA SQL port is 3<instance>13/15 or 39041 for HANA Express (HXE).

CREATE OR REPLACE CONNECTION SAPHANA_JDBC
    TO 'jdbc:sap://saphana_host:39041/'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT SAPHANA_JDBC STATEMENT 'SELECT ''Connection works'' FROM DUMMY');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.SAPHANA_TO_EXASOL(
    'SAPHANA_JDBC',     -- CONNECTION_NAME: JDBC connection to the SAP HANA source
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'MYSCHEMA', 'APP_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'MY_TABLE', 'MY_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate SAP HANA comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; fixed DECIMAL>36 -> DECIMAL(36,s), floating DECIMAL/SMALLDECIMAL -> DOUBLE), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
    'HEX',              -- BINARY_HANDLING: 'HEX' (recommended; BINARY/VARBINARY/BLOB as hex text via BINTOHEX) or 'SKIP' (load NULL)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false,              -- INCLUDE_SYSTEM_SCHEMA: false (recommended) => exclude the SYSTEM (DBA) schema; true => also migrate SYSTEM
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
