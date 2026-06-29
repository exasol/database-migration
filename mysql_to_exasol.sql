create schema if not exists database_migration;

/*
    mysql_to_exasol.sql  -  generate the statements to migrate a MySQL database to Exasol v8.

    Source: MySQL 8 / 9 (backward compatible with earlier 5.x). This script runs on the
    TARGET Exasol database, reads the SOURCE metadata through a JDBC connection and RETURNS the statements
    (CREATE SCHEMA / CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / PARTITION BY / COMMENTs / IMPORT / a final
    CONSTRAINT STATE section / optional VIEW review section / optional DATA VALIDATION). It changes nothing
    itself - review the output and run it in the order returned.

    DATA TYPE MAPPING (every MySQL type is covered - nothing is silently dropped):
      tinyint -> DECIMAL(3,0); smallint -> DECIMAL(5,0); mediumint -> DECIMAL(7,0)/(8,0 unsigned);
      int -> DECIMAL(10,0); bigint -> DECIMAL(19,0)/(20,0 unsigned); decimal(p,s) -> DECIMAL(p,s)
      (p>36 or undefined -> DECIMAL_OVERFLOW); float/double/real -> DOUBLE; bit(M) -> DECIMAL(ceil(M*log10 2),0);
      date -> DATE; datetime(p) -> TIMESTAMP(p) (full fractional precision); timestamp(p) -> TIMESTAMP(p) WITH
      LOCAL TIME ZONE (the tz-aware instant type); char(n)/varchar(n) -> CHAR/VARCHAR UTF8 (char>2000 ->
      VARCHAR); tinytext/text/mediumtext/longtext/json -> VARCHAR(2000000); enum/set -> VARCHAR (label / csv);
      binary/varbinary/*blob -> base64 text (BINARY_HANDLING); geometry & all spatial subtypes -> GEOMETRY (WKT).
      Small documented difference: time -> VARCHAR(17) (Exasol has no TIME type; MySQL TIME spans -838:59:59 ..
      838:59:59 and keeps fractional seconds as text); year -> VARCHAR(4); enum/set/json -> VARCHAR (faithful
      text). tinyint(1) -> DECIMAL(3,0) lossless by default, or BOOLEAN with TINYINT1_AS_BOOLEAN=true.
      Hard limits (the IMPORT fails loudly rather than corrupting data): a value > 2,000,000 characters (unless
      TRUNCATE_LONG_STRINGS=true); DECIMAL needing > 36 digits under DECIMAL_OVERFLOW='CAP'; a date/timestamp
      outside Exasol's 0001-01-01 .. 9999-12-31 range under TEMPORAL_OUT_OF_RANGE='FAIL' (MySQL zero-dates).

    WHY CASTS ARE NEEDED ON THE SOURCE (verified live with Connector/J 9.7): UNSIGNED integers exceed their
    signed Java type (SMALLINT UNSIGNED 60000 overflows java.lang.Short; BIGINT UNSIGNED 18446744073709551615
    and BIT(64) overflow java.lang.Long), so every unsigned integer / bit is transferred as text via CAST(.. AS
    CHAR) into a DECIMAL target; YEAR is returned as a DATE by the driver, so CAST(.. AS CHAR) yields '2025';
    TIME via CAST(.. AS CHAR) keeps the full range/fraction; binary via TO_BASE64(..) is byte-exact; spatial via
    ST_AsText(..); tinyint(1) (the driver coerces it to boolean, collapsing any non 0/1 to 1) is read via
    CAST(.. AS SIGNED) so the real integer survives.

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are not
    affected by differing source/target locale settings. Character data is stored as UTF8.

    CONSTRAINTS: PK/FK are always created DISABLED (fast, order-independent load); a final CONSTRAINT STATE
    section sets them per CONSTRAINT_STATE. Exasol uses disabled keys as optimizer/BI metadata, so
    'FORCE_DISABLE' (recommended) is fine and fastest; 'FORCE_ENABLE' makes Exasol re-validate the data.

    PARTITIONING: a single-column MySQL partition key (RANGE/LIST COLUMNS on one bare column) is mapped
    best-effort to an Exasol PARTITION BY on that column (GENERATE_PARTITION_BY); HASH/KEY/expression
    partitioning is emitted as a commented manual-review note. A MySQL partitioned table is a single logical
    table (no separate child tables), so data is never migrated twice. MySQL has no distribution/clustering
    concept, so no DISTRIBUTE BY is generated.

    Not migrated (out of scope): indexes, UNIQUE/CHECK constraints, triggers, routines, events, sequences,
    users/grants. AUTO_INCREMENT is migrated as a plain numeric column carrying its values; generated columns
    (STORED/VIRTUAL) are migrated as plain columns carrying their stored/computed values (Exasol has no computed
    columns); MySQL has no user-defined types (only ENUM/SET, resolved to VARCHAR with exact sizing).
*/
--/
create or replace script database_migration.MYSQL_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol -> e.g. MYSQL_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- filter for the source schema(s)/database(s) (system schemas always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate MySQL table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,GENERATE_PARTITION_BY        -- true/false: add a best-effort PARTITION BY from a single-column MySQL partition key; complex partitioning is emitted as a commented manual-review note
  ,BINARY_HANDLING              -- 'BASE64' (recommended; binary/blob migrated losslessly as base64 text - Exasol has no binary type) or 'SKIP' (load NULL)
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; decimal>36 -> DECIMAL(36,s); IMPORT fails for values > 36 digits), 'DOUBLE' (loads, ~15 digits) or 'VARCHAR' (lossless text)
  ,TRUNCATE_LONG_STRINGS        -- true: values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,TEMPORAL_OUT_OF_RANGE        -- 'FAIL' (recommended; IMPORT fails on a zero-date/out-of-range date), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min)
  ,TINYINT1_AS_BOOLEAN          -- false (recommended; tinyint(1) -> DECIMAL(3,0), value preserved) or true (tinyint(1) -> BOOLEAN)
  ,CHECK_MIGRATION              -- true/false: additionally emit data-validation metrics (per-table "<table>_MIG_CHK" + a "<schema>_MIG_CHK" summary comparing source vs target). Run AFTER the IMPORTs.
) RETURNS TABLE
AS

-- IDENTIFIER_CASE_INSENSITIVE = true wraps every identifier in upper(...) (stored UPPER CASE), applied
-- consistently to schemas, tables, columns, primary keys, foreign keys, partition keys, comments.
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
trunc        = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
t1bool       = (TINYINT1_AS_BOOLEAN == true) or (string.upper(tostring(TINYINT1_AS_BOOLEAN)) == 'TRUE')
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'BASE64' end
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' and decof ~= 'VARCHAR' then decof = 'CAP' end
oormode = string.upper(tostring(TEMPORAL_OUT_OF_RANGE))
if oormode ~= 'NULL' and oormode ~= 'CLAMP' then oormode = 'FAIL' end
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
-- target schema name expression (TARGET_SCHEMA override or the source schema)
if TARGET_SCHEMA == null then tschema = [["schema_name"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
-- foreign-key parent schema: when TARGET_SCHEMA is set every table lands there, otherwise the parent keeps its own schema
if TARGET_SCHEMA == null then ref_sname_e = U('"ref_schema"') else ref_sname_e = sname_e end

-- Always-excluded MySQL system schemas + the requested SCHEMA/TABLE filters (inner literals double-quoted).
myflt = [[ and table_schema not in (''mysql'',''information_schema'',''performance_schema'',''sys'') and table_schema like '']]..SCHEMA_FILTER..[['' and table_name like '']]..TABLE_FILTER..[['' ]]

-------------------------------------------------------------------------------------------------------
-- Remote (MySQL) metadata queries. Inner literals are quote-doubled (embedded in statement '...').
-------------------------------------------------------------------------------------------------------
columns_q = [[select table_schema as schema_name, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, column_type, character_maximum_length as char_len, numeric_precision as num_prec, numeric_scale as num_scale, datetime_precision as dt_prec, extra from information_schema.columns where (table_schema, table_name) in (select table_schema, table_name from information_schema.tables where table_type = ''BASE TABLE'')]]..myflt

pk_q = [[select table_schema as schema_name, table_name, column_name, ordinal_position as column_position from information_schema.key_column_usage where constraint_name = ''PRIMARY'']]..myflt

fk_q = [[select table_schema as schema_name, table_name, constraint_name as fk_name, column_name as fk_column, referenced_table_schema as ref_schema, referenced_table_name as ref_table, referenced_column_name as ref_column, ordinal_position as col_position from information_schema.key_column_usage where referenced_table_name is not null]]..myflt

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions producing the generated statement text.
-------------------------------------------------------------------------------------------------------
sc = [[(case when "num_scale" is null or "num_scale" < 0 then 0 when "num_scale" > 36 then 36 else "num_scale" end)]]
if decof == 'DOUBLE' then
	dec_t = [[case when "num_prec" is null or "num_prec" > 36 then 'DOUBLE' else 'DECIMAL(' || "num_prec" || ',' || ]]..sc..[[ || ')' end]]
elseif decof == 'VARCHAR' then
	dec_t = [[case when "num_prec" is null or "num_prec" > 36 then 'VARCHAR(2000000) ASCII' else 'DECIMAL(' || "num_prec" || ',' || ]]..sc..[[ || ')' end]]
else
	dec_t = [[case when "num_prec" is null then 'DECIMAL(36,18)' when "num_prec" > 36 then 'DECIMAL(36,' || ]]..sc..[[ || ')' else 'DECIMAL(' || "num_prec" || ',' || ]]..sc..[[ || ')' end]]
end

if t1bool then t1t = [['BOOLEAN']] else t1t = [['DECIMAL(3,0)']] end

-- Exasol column type, mapped by MySQL data_type (+ unsigned flag / display width / fractional precision).
col_t = [[case
	when "data_type" = 'tinyint' and "column_type" like 'tinyint(1)%' then ]]..t1t..[[
	when "data_type" = 'tinyint' then 'DECIMAL(3,0)'
	when "data_type" = 'smallint' then 'DECIMAL(5,0)'
	when "data_type" = 'mediumint' then case when "is_unsigned" = 1 then 'DECIMAL(8,0)' else 'DECIMAL(7,0)' end
	when "data_type" in ('int','integer') then 'DECIMAL(10,0)'
	when "data_type" = 'bigint' then case when "is_unsigned" = 1 then 'DECIMAL(20,0)' else 'DECIMAL(19,0)' end
	when "data_type" in ('decimal','numeric') then ]]..dec_t..[[
	when "data_type" in ('float','double','real') then 'DOUBLE'
	when "data_type" = 'bit' then 'DECIMAL(' || cast(floor("num_prec" * 0.30103) + 1 as decimal(3,0)) || ',0)'
	when "data_type" = 'date' then 'DATE'
	when "data_type" = 'datetime' then 'TIMESTAMP(' || coalesce("dt_prec",0) || ')'
	when "data_type" = 'timestamp' then 'TIMESTAMP(' || coalesce("dt_prec",0) || ') WITH LOCAL TIME ZONE'
	when "data_type" = 'time' then 'VARCHAR(17) ASCII'
	when "data_type" = 'year' then 'VARCHAR(4) ASCII'
	when "data_type" = 'char' then case when "char_len" > 2000 then 'VARCHAR(' || "char_len" || ') UTF8' else 'CHAR(' || "char_len" || ') UTF8' end
	when "data_type" = 'varchar' then 'VARCHAR(' || (case when "char_len" > 2000000 then 2000000 else "char_len" end) || ') UTF8'
	when "data_type" in ('tinytext','text','mediumtext','longtext') then 'VARCHAR(2000000) UTF8'
	when "data_type" in ('enum','set') then 'VARCHAR(' || (case when "char_len" is null or "char_len" < 1 then 2000000 when "char_len" > 2000000 then 2000000 else "char_len" end) || ') UTF8'
	when "data_type" = 'json' then 'VARCHAR(2000000) UTF8'
	when "data_type" in ('binary','varbinary') then 'VARCHAR(' || (case when (floor(("char_len"+2)/3))*4 > 2000000 then 2000000 when (floor(("char_len"+2)/3))*4 < 1 then 1 else (floor(("char_len"+2)/3))*4 end) || ') ASCII'
	when "data_type" in ('tinyblob','blob','mediumblob','longblob') then 'VARCHAR(2000000) ASCII'
	when "data_type" in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection') then 'GEOMETRY'
	else 'VARCHAR(2000000) UTF8'
end]]

-- DEFAULT mapping (numeric literals as-is; CURRENT_TIMESTAMP; everything else quoted; generated columns skipped).
default_e = [[case
	when "is_generated" = 1 then ''
	when "column_default" is null then ''
	when upper("extra") like '%DEFAULT_GENERATED%' then (case when upper("column_default") like 'CURRENT_TIMESTAMP%' or upper("column_default") = 'NOW()' then ' DEFAULT CURRENT_TIMESTAMP' else '' end)
	when "data_type" in ('tinyint','smallint','mediumint','int','integer','bigint','decimal','numeric','float','double','real') and "column_default" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}$' then ' DEFAULT ' || "column_default"
	else ' DEFAULT ''' || replace("column_default", '''', '''''') || ''''
end]]
coldef = [['"' || "exa_col" || '" ' || (]]..col_t..[[) || (]]..default_e..[[) || (case when "not_null" = 1 then ' NOT NULL' else '' end)]]

-- temporal source expressions (TEMPORAL_OUT_OF_RANGE; MySQL zero-dates '0000-00-00' detected via "= 0").
if oormode == 'NULL' then
	date_src = [['case when ' || "col_bt" || ' = 0 then null else ' || "col_bt" || ' end']]
	ts_src   = date_src
elseif oormode == 'CLAMP' then
	date_src = [['case when ' || "col_bt" || ' = 0 then date ''''0001-01-01'''' else ' || "col_bt" || ' end']]
	ts_src   = [['case when ' || "col_bt" || ' = 0 then timestamp ''''0001-01-01 00:00:00'''' else ' || "col_bt" || ' end']]
else
	date_src = [["col_bt"]]
	ts_src   = [["col_bt"]]
end

if binmode == 'SKIP' then bin_imp = [['cast(null as char)']] else bin_imp = [['to_base64(' || "col_bt" || ')']] end
if decof == 'VARCHAR' then num_imp = [['cast(' || "col_bt" || ' as char)']] else num_imp = [["col_bt"]] end
if trunc then text_imp = [['left(' || "col_bt" || ', 2000000)']] else text_imp = [["col_bt"]] end
if t1bool then t1src = [["col_bt"]] else t1src = [['cast(' || "col_bt" || ' as signed)']] end

-- source SELECT expression(s) for the IMPORT (must align positionally with coldef).
src = [[case
	when "data_type" = 'tinyint' and "column_type" like 'tinyint(1)%' then ]]..t1src..[[
	when "data_type" in ('tinyint','smallint','mediumint','int','integer','bigint') and "is_unsigned" = 1 then 'cast(' || "col_bt" || ' as char)'
	when "data_type" = 'bit' then 'cast(cast(' || "col_bt" || ' as unsigned) as char)'
	when "data_type" = 'year' then 'cast(' || "col_bt" || ' as char)'
	when "data_type" = 'time' then 'cast(' || "col_bt" || ' as char)'
	when "data_type" in ('binary','varbinary','tinyblob','blob','mediumblob','longblob') then ]]..bin_imp..[[
	when "data_type" in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection') then 'st_astext(' || "col_bt" || ')'
	when "data_type" = 'date' then ]]..date_src..[[
	when "data_type" in ('datetime','timestamp') then ]]..ts_src..[[
	when "data_type" in ('decimal','numeric') then ]]..num_imp..[[
	when "data_type" in ('tinytext','text','mediumtext','longtext','json') then ]]..text_imp..[[
	else "col_bt"
end]]

-- constraint-state word + trailing comment (final CONSTRAINT STATE section uses MODIFY CONSTRAINT).
if cstate == 'FORCE_ENABLE' then sw = 'enable'; scomment = [[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; scomment = [[  -- matches MySQL source (keys active)]]
else sw = 'disable'; scomment = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]
fkname = [[coalesce(nullif("fk_name",''), "table_name" || '_FK_' || "ref_table")]]
known = [["data_type" in ('tinyint','smallint','mediumint','int','integer','bigint','decimal','numeric','float','double','real','bit','date','datetime','timestamp','time','year','char','varchar','tinytext','text','mediumtext','longtext','enum','set','json','binary','varbinary','tinyblob','blob','mediumblob','longblob','geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')]]

-- optional CTEs --------------------------------------------------------------------------------------
comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select table_schema as schema_name, table_name, 0 as sub, cast(null as char) as column_name, table_comment as comment_text from information_schema.tables where table_type = ''BASE TABLE'' and table_comment <> '''' ]]..myflt..[[ union all select table_schema, table_name, ordinal_position, column_name, column_comment from information_schema.columns where column_comment <> '''' ]]..myflt..[[ and (table_schema, table_name) in (select table_schema, table_name from information_schema.tables where table_type = ''BASE TABLE'')') c ("schema_name", "table_name", "sub", "column_name", "comment_text") )
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" = 0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" > 0)]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select table_schema as schema_name, table_name as view_name, view_definition as view_def from information_schema.views where 1 = 1 ]]..myflt..[[') v ("schema_name", "view_name", "view_def") )
,vv_views as (select '-- ' || "schema_name" || '.' || "view_name" || '  (MySQL view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (MySQL SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

part_cte = ''  part_union = ''
if gen_part then
	part_cte = [[
,vv_part_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select distinct table_schema as schema_name, table_name, partition_method, partition_expression from information_schema.partitions where partition_name is not null ]]..myflt..[[') pt ("schema_name", "table_name", "partition_method", "partition_expression") )
,vv_partcol as (select "schema_name","table_name", coalesce("partition_method",'unknown') as "pm", coalesce("partition_expression",'') as "pe",
	case when "partition_expression" REGEXP_LIKE '^`{0,1}[A-Za-z_][A-Za-z0-9_]*`{0,1}$' then replace("partition_expression",'`','') else null end as "raw_col"
	from vv_part_raw)
,vv_part as (
	select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" PARTITION BY "' || ]]..U('"raw_col"')..[[ || '";' as sql_text
	from vv_partcol where "raw_col" is not null
	union all
	select '-- "' || ]]..U('"schema_name"')..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MySQL ' || "pm" || ' partitioning (' || "pe" || ') not auto-mapped - review and add PARTITION BY manually if appropriate.' as sql_text
	from vv_partcol where "raw_col" is null)]]
	part_union = "\n".. [[UNION ALL select 37, cast('-- ### PARTITION BY (best-effort from a single-column MySQL partition key; complex partitioning listed as a review note) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 38, sql_text from vv_part]]
end

-- CHECK_MIGRATION: per table a wide single-scan typed metrics row is computed on BOTH systems; a per-schema
-- summary unpivots+joins them, flagging each metric OK/DEVIATION. Metrics are cross-database comparable so
-- faithfully migrated data shows no false deviation: row/NULL/DISTINCT counts, numeric MIN/MAX/SUM (exact
-- decimals/integers only), date/timestamp MIN/MAX, variable-char length MIN/MAX. Binary/geometry get NULL/
-- DISTINCT counts only; fixed char and DOUBLE/float are excluded from the comparable metrics.
check_cte = ''  check_union = ''
if gen_check then
	chk_int  = [["data_type" in ('tinyint','smallint','mediumint','int','integer','bigint','bit')]]
	chk_dec  = [["data_type" in ('decimal','numeric') and "num_prec" between 1 and 36]]
	chk_len  = [["data_type" in ('varchar','tinytext','text','mediumtext','longtext','enum','set','json')]]
	if binmode == 'SKIP' then bin_excl = [[ and "data_type" not in ('binary','varbinary','tinyblob','blob','mediumblob','longblob') ]] else bin_excl = '' end
	distinct_excl = [[ and "data_type" not in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection','float','double','real') ]]..bin_excl
	check_cte = [[
,vv_chk_base as (
	select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord",
	       sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || x."exa_col" || '"' when x."data_type" = 'bit' then 'cast(' || x."col_bt" || ' as unsigned)' else x."col_bt" end as "ref"
	from vv_columns x
	cross join (select 'Exasol' as "db_system" union all select 'MySQL' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 8) mid
)
,vv_chk_expr as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide_name",
	       (case
	          when "metric_id" = 0 and "ordinal_position" = "min_ord" then 'cast(count(*) as decimal(36,0))'
	          when "metric_id" = 1 and "not_null" = 0 ]]..bin_excl..[[ then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))'
	          when "metric_id" = 2 and (]]..chk_int..[[) then 'cast(min(' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 2 and (]]..chk_dec..[[) then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 2 and "data_type" = 'date' then 'min(' || "ref" || ')'
	          when "metric_id" = 2 and "data_type" in ('datetime','timestamp') then (case when "db_system" = 'Exasol' then 'to_char(min(' || "ref" || '), ''YYYY-MM-DD HH24:MI:SS.FF6'')' else 'date_format(min(' || "ref" || '), ''%Y-%m-%d %H:%i:%s.%f'')' end)
	          when "metric_id" = 3 and (]]..chk_int..[[) then 'cast(max(' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 3 and (]]..chk_dec..[[) then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 3 and "data_type" = 'date' then 'max(' || "ref" || ')'
	          when "metric_id" = 3 and "data_type" in ('datetime','timestamp') then (case when "db_system" = 'Exasol' then 'to_char(max(' || "ref" || '), ''YYYY-MM-DD HH24:MI:SS.FF6'')' else 'date_format(max(' || "ref" || '), ''%Y-%m-%d %H:%i:%s.%f'')' end)
	          when "metric_id" = 4 ]]..distinct_excl..[[ then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 5 and (]]..chk_int..[[) then 'cast(sum(' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 5 and (]]..chk_dec..[[) then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 6 and (]]..chk_len..[[) then (case when "db_system" = 'Exasol' then 'cast(min(length(' || "ref" || ')) as decimal(36,0))' else 'cast(min(char_length(' || "ref" || ')) as decimal(36,0))' end)
	          when "metric_id" = 7 and (]]..chk_len..[[) then (case when "db_system" = 'Exasol' then 'cast(max(length(' || "ref" || ')) as decimal(36,0))' else 'cast(max(char_length(' || "ref" || ')) as decimal(36,0))' end)
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
	       'select ' || (case when "db_system" = 'Exasol' then 'cast(''Exasol'' as varchar(10)) as "DB_SYSTEM", ' else '''MySQL'' as db_system, ' end) || listagg("metric_expr" || (case when "db_system" = 'Exasol' then ' as "' || "metric_name" || '"' else '' end), ', ') within group (order by "ordinal_position","metric_id") || ' from ' || (case when "db_system" = 'Exasol' then '"' || "exa_schema" || '"."' || "exa_table" || '"' else '`' || "schema_name" || '`.`' || "table_name" || '`' end) as "sys_select"
	from vv_chk_named group by "exa_schema","exa_table","schema_name","table_name","wide_name","db_system"
)
,vv_chk_wide as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "wide_name" || '" AS ' || max(case when "db_system" = 'Exasol' then "sys_select" end) || ' UNION ALL select * from (IMPORT FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || replace(max(case when "db_system" = 'MySQL' then "sys_select" end), '''', '''''') || '''' || ') ;' as sql_text
	from vv_chk_sys group by "exa_schema","wide_name"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system",
	       'select ' || '''' || "exa_table" || '''' || ' as "TABLE_NAME", ' || '''' || "metric_name" || '''' || ' as "METRIC", to_char("' || "metric_name" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide_name" || '" where "DB_SYSTEM" = ' || '''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'CREATE OR REPLACE TABLE "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" AS select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", t."VAL" as "MYSQL_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(t."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || listagg(case when "db_system" = 'Exasol' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') e join (' || listagg(case when "db_system" = 'MySQL' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') t on e."TABLE_NAME" = t."TABLE_NAME" and e."METRIC" = t."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
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
	select ]]..sname_e..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table", ]]..U('"column_name"')..[[ as "exa_col", '`' || "column_name" || '`' as "col_bt",
	       case when "is_nullable" = 'NO' then 1 else 0 end as "not_null",
	       case when "column_type" like '%unsigned%' then 1 else 0 end as "is_unsigned",
	       case when upper("extra") like '%GENERATED%' and upper("extra") not like '%DEFAULT_GENERATED%' then 1 else 0 end as "is_generated",
	       t.*
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..columns_q..[[') t ("schema_name", "table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "column_type", "char_len", "num_prec", "num_scale", "dt_prec", "extra")
)
,vv_catchall as (
	select '-- NOTE: column "' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" has unmapped MySQL type ' || "data_type" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
	from vv_columns where not (]]..known..[[)
)
,vv_pk_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..pk_q..[[') p ("schema_name", "table_name", "column_name", "column_position"))
,vv_pk as (
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" PRIMARY KEY (' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "column_position") || ') DISABLE;' as sql_text
	from vv_pk_raw group by "schema_name","table_name"
)
,vv_fk_raw as (select f.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..fk_q..[[') f ("schema_name", "table_name", "fk_name", "fk_column", "ref_schema", "ref_table", "ref_column", "col_position") where exists (select 1 from vv_columns c where c."schema_name" = f."ref_schema" and c."table_name" = f."ref_table"))
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
	select 'IMPORT INTO "' || "exa_schema" || '"."' || "exa_table" || '" FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || 'select ' || group_concat((]]..src..[[) order by "ordinal_position" separator ', ') || ' from ' || '`' || "schema_name" || '`.`' || "table_name" || '`' || '''' || ';' as sql_text
	from vv_columns group by "exa_schema","exa_table","schema_name","table_name"
)]]..comments_cte..views_cte..part_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_catchall
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 3, cast('-- ### TABLES (incl. PRIMARY KEY, created DISABLED) ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk
	UNION ALL select 7, cast('-- ### FOREIGN KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 8, sql_text from vv_fk]]..comments_union..part_union..[[
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
--   * The MySQL database must be reachable from this Exasol database.
--   * The credentials used in the connection must be valid.
--   * Use the latest MySQL JDBC driver (mysql-connector-j).
--
-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * mysql-connector-j 9.x or higher
--       https://mvnrepository.com/artifact/com.mysql/mysql-connector-j
--   * Driver setup guide:
--       https://docs.exasol.com/db/latest/loading_data/connect_sources/mysql.htm
--   * Optional: add the URL parameter zeroDateTimeBehavior=CONVERT_TO_NULL to load MySQL zero-dates as NULL at
--     the driver level (otherwise the recommended TEMPORAL_OUT_OF_RANGE='FAIL' lets the IMPORT fail loudly on them).
--
-- Create a connection to the MySQL database (adjust host, database name and credentials),
-- then run the accompanying test query.

CREATE OR REPLACE CONNECTION MYSQL_JDBC
    TO 'jdbc:mysql://mysql_host_or_ip:3306/my_database'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT MYSQL_JDBC STATEMENT 'SELECT ''Connection works''');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.MYSQL_TO_EXASOL(
    'MYSQL_JDBC',       -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source database(s): 'mydb', 'sales_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate MySQL comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from a single-column MySQL partition key; complex partitioning is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; binary/blob migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; decimal>36 -> DECIMAL(36,s); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'FAIL',             -- TEMPORAL_OUT_OF_RANGE: 'FAIL' (recommended; IMPORT fails on a zero-date / out-of-range date), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min)
    false,              -- TINYINT1_AS_BOOLEAN: false (recommended; tinyint(1) -> DECIMAL(3,0), value preserved) or true (tinyint(1) -> BOOLEAN)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
