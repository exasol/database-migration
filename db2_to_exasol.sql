create schema if not exists database_migration;

/*
    db2_to_exasol.sql  -  generate the statements to migrate an IBM Db2 (LUW) database to Exasol v8.

    Source: IBM Db2 for Linux/Unix/Windows 11.x / 12.x. This script runs on the TARGET Exasol database, reads the
    SOURCE metadata through a JDBC connection (native SYSCAT catalog) and RETURNS the statements (CREATE SCHEMA /
    CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / PARTITION BY / DISTRIBUTE BY / COMMENTs / IMPORT / a final
    CONSTRAINT STATE section / optional VIEW review section / optional DATA VALIDATION). It changes nothing
    itself - review the output and run it in the order returned.

    DATA TYPE MAPPING (every Db2 type is covered; verified live on Db2 12.1.5 + jcc 12.1.5.0):
      SMALLINT/INTEGER/BIGINT -> DECIMAL(5/10/19,0); DECIMAL/NUMERIC(p,s) -> DECIMAL(p,s) (Db2 p<=31);
      DECFLOAT -> VARCHAR (lossless) or DOUBLE (DECFLOAT_HANDLING); REAL/DOUBLE -> DOUBLE; DATE -> DATE;
      TIME -> VARCHAR(8) (HH:MM:SS); TIMESTAMP(p) -> TIMESTAMP(min(p,9)) (Exasol max 9); CHAR/VARCHAR -> CHAR/
      VARCHAR UTF8 (char>2000 -> VARCHAR); CLOB/LONG VARCHAR -> VARCHAR(2000000); GRAPHIC/VARGRAPHIC -> CHAR/
      VARCHAR UTF8; DBCLOB -> VARCHAR; BOOLEAN -> BOOLEAN; XML -> VARCHAR (XMLSERIALIZE).
      Binary (CHAR/VARCHAR FOR BIT DATA, BINARY, VARBINARY, BLOB, ROWID) -> hex text (BINARY_HANDLING).
      DISTINCT-type UDTs are resolved to their source built-in type via SYSCAT.DATATYPES and migrated as that base.
      Hard limits (the IMPORT fails loudly rather than corrupting data): CLOB/DBCLOB value > 2,000,000 chars
      (unless TRUNCATE_LONG_STRINGS); a binary value > 16,336 bytes (the Db2 HEX/VARCHAR limit - FOR BIT DATA
      fails loudly, BLOB is truncated; use BINARY_HANDLING='SKIP' to load NULL instead). TIMESTAMP fractional
      precision > 9 is capped to 9 (Exasol's maximum).

    WHY CASTS/FUNCTIONS ARE NEEDED ON THE SOURCE (verified live): the jcc driver cannot transfer DECFLOAT,
    GRAPHIC/VARGRAPHIC/DBCLOB, BLOB or DISTINCT-UDT values directly ("unknown JDBC type"), so they are read via
    CAST(.. AS VARCHAR/base), HEX(..) etc.; Db2 has no base64 function, so binary is encoded as HEX (lossless);
    TIME is read via REPLACE(CHAR(..),'.',':') -> HH:MM:SS; XML via XMLSERIALIZE; column aliases are ignored by
    the driver, so every metadata IMPORT carries an explicit derived column list. CLOB/BOOLEAN/DATE/TIMESTAMP/
    DECIMAL/integers/CHAR/VARCHAR transfer directly.

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are not
    affected by differing source/target locale settings. Character data is stored as UTF8.

    CONSTRAINTS: PK/FK are always created DISABLED (fast, order-independent load); a final CONSTRAINT STATE
    section sets them per CONSTRAINT_STATE.

    PARTITIONING / DISTRIBUTION: a single-column Db2 range-partition key is mapped to an Exasol PARTITION BY
    (GENERATE_PARTITION_BY); the Db2 DISTRIBUTE BY HASH key is mapped to an Exasol DISTRIBUTE BY
    (GENERATE_DISTRIBUTION_BY, default true). Both verified live. Complex / multi-column / expression range
    partitioning is emitted as a commented manual-review note.

    Not migrated (out of scope): indexes, UNIQUE/CHECK constraints, triggers, routines, sequences, MQTs, packages.
    IDENTITY and GENERATED columns are migrated as plain columns carrying their values (Exasol has no computed
    columns). Always-excluded: the Db2 system schemas (SYS*, NULLID).
*/
--/
create or replace script database_migration.DB2_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol -> e.g. DB2_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended; Db2 folds unquoted names to UPPER) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- filter for the source schemas (system schemas always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate Db2 table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,GENERATE_PARTITION_BY        -- true/false: add a best-effort PARTITION BY from a single-column Db2 range-partition key; complex partitioning is emitted as a commented manual-review note
  ,GENERATE_DISTRIBUTION_BY     -- true/false (default true): add a DISTRIBUTE BY from the Db2 DISTRIBUTE BY HASH (DPF) distribution key
  ,BINARY_HANDLING              -- 'HEX' (recommended; binary/blob migrated losslessly as hex text - Db2 has no base64) or 'SKIP' (load NULL)
  ,DECFLOAT_HANDLING            -- 'VARCHAR' (recommended; DECFLOAT as lossless text - keeps all 16/34 digits) or 'DOUBLE' (numeric, ~15-16 significant digits)
  ,TRUNCATE_LONG_STRINGS        -- true: values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,CHECK_MIGRATION              -- true/false: additionally emit data-validation metrics (per-table "<table>_MIG_CHK" + a "<schema>_MIG_CHK" summary comparing source vs target). Run AFTER the IMPORTs.
) RETURNS TABLE
AS

exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

-- Normalize option parameters.
cstate = string.upper(tostring(CONSTRAINT_STATE))
if cstate ~= 'SET_AS_SOURCE' and cstate ~= 'FORCE_ENABLE' then cstate = 'FORCE_DISABLE' end
gen_comments = (GENERATE_COMMENTS == true) or (string.upper(tostring(GENERATE_COMMENTS)) == 'TRUE')
gen_views    = (GENERATE_VIEWS == true) or (string.upper(tostring(GENERATE_VIEWS)) == 'TRUE')
gen_part     = (GENERATE_PARTITION_BY == true) or (string.upper(tostring(GENERATE_PARTITION_BY)) == 'TRUE')
gen_dist = true   -- DISTRIBUTE BY defaults to true; only an explicit false/FALSE disables it
if (GENERATE_DISTRIBUTION_BY == false) or (string.upper(tostring(GENERATE_DISTRIBUTION_BY)) == 'FALSE') then gen_dist = false end
trunc        = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'HEX' end
decfmode = string.upper(tostring(DECFLOAT_HANDLING))
if decfmode ~= 'DOUBLE' then decfmode = 'VARCHAR' end
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
if TARGET_SCHEMA == null then tschema = [["schema_name"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
if TARGET_SCHEMA == null then ref_sname_e = U('"ref_schema"') else ref_sname_e = sname_e end

-- Db2 filter (system schemas SYS*/NULLID always excluded) - parameterized by the schema/table column refs.
function db2flt(s, tn) return [[ and rtrim(]]..s..[[) not like ''SYS%'' and rtrim(]]..s..[[) <> ''NULLID'' and rtrim(]]..s..[[) like '']]..SCHEMA_FILTER..[['' and ]]..tn..[[ like '']]..TABLE_FILTER..[['' ]] end

-------------------------------------------------------------------------------------------------------
-- Remote (Db2) metadata queries (native SYSCAT). Inner literals are quote-doubled (embedded in statement '...').
-- DISTINCT-type UDTs are resolved to their source built-in via SYSCAT.DATATYPES (METATYPE 'T').
-------------------------------------------------------------------------------------------------------
columns_q = [[select rtrim(c.TABSCHEMA), c.TABNAME, c.COLNAME, c.COLNO, c.NULLS, c.GENERATED, c.IDENTITY, CAST(c."DEFAULT" AS VARCHAR(254)),
	case when c.TYPESCHEMA <> ''SYSIBM '' and c.TYPESCHEMA <> ''SYSIBM'' then 1 else 0 end,
	coalesce(d.SOURCENAME, c.TYPENAME), coalesce(d.LENGTH, c.LENGTH), coalesce(d.SCALE, c.SCALE), coalesce(d.CODEPAGE, c.CODEPAGE), c.PARTKEYSEQ
	from SYSCAT.COLUMNS c left join SYSCAT.DATATYPES d on d.TYPESCHEMA = c.TYPESCHEMA and d.TYPENAME = c.TYPENAME and d.METATYPE = ''T''
	where (c.TABSCHEMA, c.TABNAME) in (select TABSCHEMA, TABNAME from SYSCAT.TABLES where TYPE = ''T'']]..db2flt('TABSCHEMA','TABNAME')..[[)]]

pk_q = [[select rtrim(k.TABSCHEMA), k.TABNAME, k.COLNAME, k.COLSEQ from SYSCAT.KEYCOLUSE k join SYSCAT.TABCONST t on t.CONSTNAME = k.CONSTNAME and t.TABSCHEMA = k.TABSCHEMA and t.TABNAME = k.TABNAME where t.TYPE = ''P'']]..db2flt('k.TABSCHEMA','k.TABNAME')

fk_q = [[select rtrim(r.TABSCHEMA), r.TABNAME, r.CONSTNAME, kf.COLNAME, kf.COLSEQ, rtrim(r.REFTABSCHEMA), r.REFTABNAME, kp.COLNAME
	from SYSCAT.REFERENCES r
	join SYSCAT.KEYCOLUSE kf on kf.CONSTNAME = r.CONSTNAME and kf.TABSCHEMA = r.TABSCHEMA and kf.TABNAME = r.TABNAME
	join SYSCAT.KEYCOLUSE kp on kp.CONSTNAME = r.REFKEYNAME and kp.TABSCHEMA = r.REFTABSCHEMA and kp.TABNAME = r.REFTABNAME and kp.COLSEQ = kf.COLSEQ
	where 1 = 1]]..db2flt('r.TABSCHEMA','r.TABNAME')

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions producing the generated statement text.
-------------------------------------------------------------------------------------------------------
sc = [[(case when "scale" is null or "scale" < 0 then 0 when "scale" > 31 then 31 else "scale" end)]]
if decfmode == 'DOUBLE' then decfloat_t = [['DOUBLE']] else decfloat_t = [['VARCHAR(45) ASCII']] end

-- Exasol column type, mapped by the (resolved) Db2 type name + codepage + length/scale.
col_t = [[case
	when "type_name" = 'SMALLINT' then 'DECIMAL(5,0)'
	when "type_name" = 'INTEGER' then 'DECIMAL(10,0)'
	when "type_name" = 'BIGINT' then 'DECIMAL(19,0)'
	when "type_name" in ('DECIMAL','NUMERIC') then 'DECIMAL(' || "len" || ',' || ]]..sc..[[ || ')'
	when "type_name" = 'DECFLOAT' then ]]..decfloat_t..[[
	when "type_name" in ('REAL','DOUBLE') then 'DOUBLE'
	when "type_name" = 'DATE' then 'DATE'
	when "type_name" = 'TIME' then 'VARCHAR(8) ASCII'
	when "type_name" = 'TIMESTAMP' then 'TIMESTAMP(' || (case when coalesce("scale",6) > 9 then 9 else coalesce("scale",6) end) || ')'
	when "type_name" = 'CHARACTER' and "codepage" = 0 then 'VARCHAR(' || (case when 2*"len" > 2000000 then 2000000 else 2*"len" end) || ') ASCII'
	when "type_name" = 'CHARACTER' then case when "len" > 2000 then 'VARCHAR(' || "len" || ') UTF8' else 'CHAR(' || "len" || ') UTF8' end
	when "type_name" = 'VARCHAR' and "codepage" = 0 then 'VARCHAR(' || (case when 2*"len" > 2000000 then 2000000 else 2*"len" end) || ') ASCII'
	when "type_name" = 'VARCHAR' then 'VARCHAR(' || (case when "len" > 2000000 then 2000000 else "len" end) || ') UTF8'
	when "type_name" in ('CLOB','LONG VARCHAR') then 'VARCHAR(2000000) UTF8'
	when "type_name" = 'GRAPHIC' then case when "len" > 2000 then 'VARCHAR(' || "len" || ') UTF8' else 'CHAR(' || "len" || ') UTF8' end
	when "type_name" = 'VARGRAPHIC' then 'VARCHAR(' || (case when "len" > 2000000 then 2000000 else "len" end) || ') UTF8'
	when "type_name" in ('DBCLOB','LONG VARGRAPHIC') then 'VARCHAR(2000000) UTF8'
	when "type_name" in ('BLOB','BINARY','VARBINARY') then 'VARCHAR(' || (case when 2*"len" > 2000000 then 2000000 else 2*"len" end) || ') ASCII'
	when "type_name" = 'XML' then 'VARCHAR(2000000) UTF8'
	when "type_name" = 'BOOLEAN' then 'BOOLEAN'
	when "type_name" = 'ROWID' then 'VARCHAR(' || (case when "len" is null or "len" < 1 then 40 else 2*"len" end) || ') ASCII'
	else 'VARCHAR(2000000) UTF8'
end]]

-- DEFAULT mapping (Db2 stores SQL-literal defaults: 'NEW' quoted, CURRENT TIMESTAMP keyword). Pass literals
-- through as-is; map CURRENT TIMESTAMP; skip other expression defaults and generated/identity columns.
default_e = [[case
	when "is_generated" = 1 then ''
	when "default_value" is null then ''
	when upper(trim("default_value")) in ('CURRENT TIMESTAMP','CURRENT_TIMESTAMP') then ' DEFAULT CURRENT_TIMESTAMP'
	when "default_value" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}$' then ' DEFAULT ' || "default_value"
	when "default_value" REGEXP_LIKE '^''.*''$' then ' DEFAULT ' || "default_value"
	else ''
end]]
coldef = [['"' || "exa_col" || '" ' || (]]..col_t..[[) || (]]..default_e..[[) || (case when "not_null" = 1 then ' NOT NULL' else '' end)]]

-- import source expressions (binary / decfloat / long-string handling)
if binmode == 'SKIP' then hex_imp = [['cast(null as varchar(10))']] else hex_imp = [['HEX(' || "col_q" || ')']] end
if binmode == 'SKIP' then blob_imp = [['cast(null as varchar(10))']] else blob_imp = [['HEX(CAST(' || "col_q" || ' AS VARCHAR(16336) FOR BIT DATA))']] end
if decfmode == 'DOUBLE' then decfloat_imp = [['cast(' || "col_q" || ' as double)']] else decfloat_imp = [['cast(' || "col_q" || ' as varchar(45))']] end
if trunc then text_imp = [['LEFT(' || "col_q" || ', 2000000)']] else text_imp = [["col_q"]] end

-- source SELECT expression(s) for the IMPORT (must align positionally with coldef).
src = [[case
	when "is_udt" = 1 then 'CAST(' || "col_q" || ' AS ' || "udt_cast" || ')'
	when "type_name" = 'DECFLOAT' then ]]..decfloat_imp..[[
	when "type_name" = 'TIME' then 'replace(char(' || "col_q" || '),''''.'''','''':'''')'
	when "type_name" in ('GRAPHIC','VARGRAPHIC') then 'cast(' || "col_q" || ' as varchar(' || (case when 4*"len" > 32672 then 32672 else 4*"len" end) || '))'
	when "type_name" in ('DBCLOB','LONG VARGRAPHIC') then 'cast(' || "col_q" || ' as varchar(32672))'
	when "type_name" = 'XML' then 'XMLSERIALIZE(' || "col_q" || ' AS CLOB(2000000))'
	when "type_name" = 'BLOB' then ]]..blob_imp..[[
	when ("type_name" in ('CHARACTER','VARCHAR') and "codepage" = 0) or "type_name" in ('BINARY','VARBINARY','ROWID') then ]]..hex_imp..[[
	when "type_name" in ('CLOB','LONG VARCHAR') then ]]..text_imp..[[
	else "col_q"
end]]

if cstate == 'FORCE_ENABLE' then sw = 'enable'; scomment = [[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; scomment = [[  -- matches Db2 source (keys active)]]
else sw = 'disable'; scomment = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]
fkname = [[coalesce(nullif("fk_name",''), "table_name" || '_FK_' || "ref_table")]]
known = [["type_name" in ('SMALLINT','INTEGER','BIGINT','DECIMAL','NUMERIC','DECFLOAT','REAL','DOUBLE','DATE','TIME','TIMESTAMP','CHARACTER','VARCHAR','CLOB','LONG VARCHAR','GRAPHIC','VARGRAPHIC','DBCLOB','LONG VARGRAPHIC','BLOB','BINARY','VARBINARY','XML','BOOLEAN','ROWID')]]

-- optional CTEs --------------------------------------------------------------------------------------
comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select rtrim(TABSCHEMA), TABNAME, 0, cast(null as varchar(128)), REMARKS from SYSCAT.TABLES where TYPE = ''T'' and REMARKS is not null]]..db2flt('TABSCHEMA','TABNAME')..[[ union all select rtrim(TABSCHEMA), TABNAME, COLNO, COLNAME, REMARKS from SYSCAT.COLUMNS where REMARKS is not null and (TABSCHEMA,TABNAME) in (select TABSCHEMA,TABNAME from SYSCAT.TABLES where TYPE = ''T'']]..db2flt('TABSCHEMA','TABNAME')..[[)') c ("schema_name","table_name","sub","column_name","comment_text") )
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" = 0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" > 0)]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select rtrim(VIEWSCHEMA), VIEWNAME, CAST(TEXT AS VARCHAR(30000)) from SYSCAT.VIEWS where 1 = 1]]..db2flt('VIEWSCHEMA','VIEWNAME')..[[') v ("schema_name","view_name","view_def") )
,vv_views as (select '-- ' || "schema_name" || '.' || "view_name" || '  (Db2 view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (Db2 SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

part_cte = ''  part_union = ''
if gen_part then
	part_cte = [[
,vv_part_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select rtrim(TABSCHEMA), TABNAME, DATAPARTITIONKEYSEQ, CAST(DATAPARTITIONEXPRESSION AS VARCHAR(500)) from SYSCAT.DATAPARTITIONEXPRESSION where 1 = 1]]..db2flt('TABSCHEMA','TABNAME')..[[') pt ("schema_name","table_name","keyseq","pexpr") )
,vv_part as (
	select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" PARTITION BY "' || ]]..U('trim("pexpr")')..[[ || '";' as sql_text
	from vv_part_raw where "keyseq" = 1 and trim("pexpr") REGEXP_LIKE '^[A-Za-z_][A-Za-z0-9_]*$'
	union all
	select '-- "' || ]]..U('"schema_name"')..[[ || '"."' || ]]..U('"table_name"')..[[ || '" Db2 partitioning (' || "pexpr" || ') not auto-mapped - review and add PARTITION BY manually if appropriate.' as sql_text
	from vv_part_raw where not (trim("pexpr") REGEXP_LIKE '^[A-Za-z_][A-Za-z0-9_]*$'))]]
	part_union = "\n".. [[UNION ALL select 37, cast('-- ### PARTITION BY (best-effort from a single-column Db2 range key; complex partitioning listed as a review note) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 38, sql_text from vv_part]]
end

dist_cte = ''  dist_union = ''
if gen_dist then
	dist_cte = [[
,vv_dist as (
	select 'ALTER TABLE "' || "exa_schema" || '"."' || "exa_table" || '" DISTRIBUTE BY ' || group_concat('"' || "exa_col" || '"' order by "partkeyseq") || ';  -- from the Db2 DISTRIBUTE BY HASH key' as sql_text
	from vv_columns where "partkeyseq" > 0 group by "exa_schema","exa_table")]]
	dist_union = "\n".. [[UNION ALL select 39, cast('-- ### DISTRIBUTE BY (from the Db2 DISTRIBUTE BY HASH / DPF key) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 40, sql_text from vv_dist]]
end

-- CHECK_MIGRATION: per table a wide single-scan typed metrics row is computed on BOTH systems; a per-schema
-- summary unpivots+joins them, flagging each metric OK/DEVIATION. Robust cross-database metrics only: row count,
-- per-column NULL counts, numeric MIN/MAX/SUM (exact integers/decimals, non-UDT) and date/timestamp MIN/MAX
-- (full-precision text). (Db2 disallows DISTINCT/length on LOBs, so those metrics are omitted.)
check_cte = ''  check_union = ''
if gen_check then
	chk_num = [["is_udt" = 0 and "type_name" in ('SMALLINT','INTEGER','BIGINT','DECIMAL','NUMERIC')]]
	if binmode == 'SKIP' then bin_excl = [[ and not (("type_name" in ('CHARACTER','VARCHAR') and "codepage"=0) or "type_name" in ('BLOB','BINARY','VARBINARY','ROWID')) ]] else bin_excl = '' end
	check_cte = [[
,vv_chk_base as (
	select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord", sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || x."exa_col" || '"' else x."col_q" end as "ref"
	from vv_columns x
	cross join (select 'Exasol' as "db_system" union all select 'DB2' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 5) mid
)
,vv_chk_expr as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide_name",
	       (case
	          when "metric_id" = 0 and "ordinal_position" = "min_ord" then (case when "db_system"='Exasol' then 'cast(count(*) as decimal(36,0))' else 'cast(count(*) as decimal(31,0))' end)
	          when "metric_id" = 1 and "not_null" = 0 ]]..bin_excl..[[ then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(' || (case when "db_system"='Exasol' then '36' else '31' end) || ',0))'
	          when "metric_id" = 2 and (]]..chk_num..[[) then 'cast(min(' || "ref" || ') as decimal(' || (case when "db_system"='Exasol' then '36' else '31' end) || ',' || ]]..sc..[[ || '))'
	          when "metric_id" = 2 and "is_udt"=0 and "type_name"='DATE' then (case when "db_system"='Exasol' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD'')' else 'varchar_format(min(' || "ref" || '),''YYYY-MM-DD'')' end)
	          when "metric_id" = 2 and "is_udt"=0 and "type_name"='TIMESTAMP' then (case when "db_system"='Exasol' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF6'')' else 'varchar_format(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF6'')' end)
	          when "metric_id" = 3 and (]]..chk_num..[[) then 'cast(max(' || "ref" || ') as decimal(' || (case when "db_system"='Exasol' then '36' else '31' end) || ',' || ]]..sc..[[ || '))'
	          when "metric_id" = 3 and "is_udt"=0 and "type_name"='DATE' then (case when "db_system"='Exasol' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD'')' else 'varchar_format(max(' || "ref" || '),''YYYY-MM-DD'')' end)
	          when "metric_id" = 3 and "is_udt"=0 and "type_name"='TIMESTAMP' then (case when "db_system"='Exasol' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF6'')' else 'varchar_format(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS.FF6'')' end)
	          when "metric_id" = 4 and (]]..chk_num..[[) then 'cast(sum(' || "ref" || ') as decimal(' || (case when "db_system"='Exasol' then '36' else '31' end) || ',' || ]]..sc..[[ || '))'
	        end) as "metric_expr"
	from vv_chk_base
)
,vv_chk_named as (
	select "exa_schema","exa_table","schema_name","table_name","ordinal_position","db_system","metric_id","wide_name","metric_expr",
	       (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_SUM' end) as "metric_name"
	from vv_chk_expr where "metric_expr" is not null
)
,vv_chk_sys as (
	select "exa_schema","exa_table","schema_name","table_name","wide_name","db_system",
	       'select ' || (case when "db_system" = 'Exasol' then 'cast(''Exasol'' as varchar(10)) as "DB_SYSTEM", ' else '''DB2'' as db_system, ' end) || listagg("metric_expr" || (case when "db_system" = 'Exasol' then ' as "' || "metric_name" || '"' else '' end), ', ') within group (order by "ordinal_position","metric_id") || ' from ' || (case when "db_system" = 'Exasol' then '"' || "exa_schema" || '"."' || "exa_table" || '"' else '"' || "schema_name" || '"."' || "table_name" || '"' end) as "sys_select"
	from vv_chk_named group by "exa_schema","exa_table","schema_name","table_name","wide_name","db_system"
)
,vv_chk_wide as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "wide_name" || '" AS ' || max(case when "db_system" = 'Exasol' then "sys_select" end) || ' UNION ALL select * from (IMPORT FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || replace(max(case when "db_system" = 'DB2' then "sys_select" end), '''', '''''') || '''' || ') ;' as sql_text
	from vv_chk_sys group by "exa_schema","wide_name"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system",
	       'select ' || '''' || "exa_table" || '''' || ' as "TABLE_NAME", ' || '''' || "metric_name" || '''' || ' as "METRIC", to_char("' || "metric_name" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide_name" || '" where "DB_SYSTEM" = ' || '''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'CREATE OR REPLACE TABLE "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" AS select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", t."VAL" as "DB2_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(t."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || listagg(case when "db_system" = 'Exasol' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') e join (' || listagg(case when "db_system" = 'DB2' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') t on e."TABLE_NAME" = t."TABLE_NAME" and e."METRIC" = t."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
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
	       case when "nulls" = 'N' then 1 else 0 end as "not_null",
	       case when "generated" in ('A','D') then 1 else 0 end as "is_generated",
	       (case when "is_udt" = 1 then (case when "type_name" in ('DECIMAL','NUMERIC') then 'DECIMAL(' || "len" || ',' || ]]..sc..[[ || ')' when "type_name" = 'VARCHAR' then 'VARCHAR(' || "len" || ')' when "type_name" = 'CHARACTER' then 'CHAR(' || "len" || ')' when "type_name" in ('GRAPHIC','VARGRAPHIC') then 'VARCHAR(' || (4*"len") || ')' else "type_name" end) else null end) as "udt_cast",
	       t.*
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..columns_q..[[') t ("schema_name","table_name","column_name","ordinal_position","nulls","generated","identity","default_value","is_udt","type_name","len","scale","codepage","partkeyseq")
)
,vv_catchall as (
	select '-- NOTE: column "' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" has unmapped Db2 type ' || "type_name" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
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
)]]..comments_cte..views_cte..part_cte..dist_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_catchall
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 3, cast('-- ### TABLES (incl. PRIMARY KEY, created DISABLED) ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk
	UNION ALL select 7, cast('-- ### FOREIGN KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 8, sql_text from vv_fk]]..comments_union..part_union..dist_union..[[
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
--   * The Db2 database must be reachable from this Exasol database.
--   * Use the IBM Data Server Driver for JDBC and SQLJ (jcc).
--
-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * jcc 12.x ( https://mvnrepository.com/artifact/com.ibm.db2/jcc )
--   * Driver setup guide: https://docs.exasol.com/db/latest/loading_data/connect_sources/db2.htm
--
-- Create a connection to the Db2 database (adjust host, database name and credentials),
-- then run the accompanying test query.

CREATE OR REPLACE CONNECTION DB2_JDBC
    TO 'jdbc:db2://db2_host_or_ip:50000/my_database'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT DB2_JDBC STATEMENT 'SELECT ''Connection works'' FROM SYSIBM.SYSDUMMY1');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.DB2_TO_EXASOL(
    'DB2_JDBC',         -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'DB2INST1', 'APP_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'MY_TABLE', 'MY_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Db2 comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => best-effort PARTITION BY from a single-column Db2 range key; complex partitioning is listed as a commented review note; false => skip
    true,               -- GENERATE_DISTRIBUTION_BY: true (default) => add DISTRIBUTE BY from the Db2 DISTRIBUTE BY HASH key; false => skip
    'HEX',              -- BINARY_HANDLING: 'HEX' (recommended; binary/blob migrated losslessly as hex text - Db2 has no base64) or 'SKIP' (load NULL)
    'VARCHAR',          -- DECFLOAT_HANDLING: 'VARCHAR' (recommended; lossless text, keeps all 16/34 digits) or 'DOUBLE' (~15-16 significant digits)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
