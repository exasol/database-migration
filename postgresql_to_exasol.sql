create schema if not exists database_migration;

/*
    postgresql_to_exasol.sql  -  generate the statements to migrate a PostgreSQL database to Exasol v8.

    Source: PostgreSQL 18 (backward compatible with earlier versions). This script runs on the TARGET Exasol
    database, reads the SOURCE metadata through a JDBC connection and RETURNS the statements (CREATE SCHEMA /
    CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY / PARTITION BY / COMMENTs / IMPORT / a final CONSTRAINT STATE
    section / optional VIEW review section / optional DATA VALIDATION). It changes nothing itself - review the
    output and run it in the order returned.

    DATA TYPE MAPPING (mapped by pg_type category, so every type is covered):
      smallint/integer/bigint -> DECIMAL(5/10/19,0); numeric(p,s) -> DECIMAL(p,s) (p>36 or unconstrained ->
      DECIMAL_OVERFLOW); real/double precision -> DOUBLE; oid -> DECIMAL(10,0); money -> DECIMAL(20,2);
      boolean -> BOOLEAN; char/varchar/text/name -> CHAR/VARCHAR UTF8 (char>2000 -> VARCHAR); date -> DATE;
      timestamp(p) -> TIMESTAMP(p) (full precision); timestamptz(p) -> TIMESTAMP(p) WITH LOCAL TIME ZONE (UTC
      instant); uuid -> CHAR(36); bytea -> base64 text (BINARY_HANDLING).
      Small documented difference: time / time with time zone -> VARCHAR (Exasol has no TIME type; offset kept
      as text); interval -> VARCHAR or native Exasol INTERVAL (INTERVAL_HANDLING); json/jsonb/xml, arrays,
      ranges/multiranges, enums, geometric, network, bit, tsvector, composite, ... -> VARCHAR (faithful text).
      Hard limits (the IMPORT fails loudly rather than corrupting data): a value > 2,000,000 characters (unless
      TRUNCATE_LONG_STRINGS=true); DECIMAL/NUMERIC needing > 36 digits under DECIMAL_OVERFLOW='CAP'; a date /
      timestamp outside Exasol's 0001-01-01 .. 9999-12-31 range under TEMPORAL_OUT_OF_RANGE='FAIL'.

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are
    not affected by differing source/target locale settings.

    CONSTRAINTS: PK/FK are always created DISABLED (fast, order-independent load); a final CONSTRAINT STATE
    section sets them per CONSTRAINT_STATE. Exasol uses disabled keys as optimizer/BI metadata, so
    'FORCE_DISABLE' (recommended) is fine and fastest; 'FORCE_ENABLE' makes Exasol re-validate the data.

    PARTITIONING: a single-column PostgreSQL partition key (declarative RANGE/LIST/HASH) is mapped best-effort
    to an Exasol PARTITION BY on that column (GENERATE_PARTITION_BY); multi-column or expression partitioning is
    emitted as a commented manual-review note. Partition CHILD tables are skipped (the parent already holds all
    rows) so data is never migrated twice. PostgreSQL has no distribution/clustering-key concept, so no
    DISTRIBUTE BY is generated.

    Not migrated (out of scope): indexes, UNIQUE/CHECK/EXCLUSION constraints, sequences, identity is migrated as
    a plain numeric column carrying its values, generated columns are migrated as plain columns carrying their
    stored values (Exasol has no computed columns), functions/procedures/triggers, users/roles/privileges.
*/
--/
create or replace script database_migration.POSTGRESQL_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol -> e.g. POSTGRESQL_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended; PostgreSQL folds unquoted names to lower-case) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- filter for the source schemas (system schemas always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source schema name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate PostgreSQL comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,GENERATE_PARTITION_BY        -- true/false: add a best-effort PARTITION BY from the PostgreSQL partition key (single column); complex partitioning is emitted as a commented manual-review note
  ,BINARY_HANDLING              -- 'BASE64' (recommended; bytea migrated losslessly as base64 text - Exasol has no binary type) or 'SKIP' (load NULL)
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; numeric>36 -> DECIMAL(36,s), unconstrained -> DECIMAL(36,18); IMPORT fails for values > 36 digits), 'DOUBLE' (loads, ~15 digits) or 'VARCHAR' (lossless text)
  ,TRUNCATE_LONG_STRINGS        -- true: values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,INTERVAL_HANDLING            -- 'VARCHAR' (recommended; interval as lossless text) or 'INTERVAL' (native Exasol INTERVAL DAY TO SECOND, best-effort - mixed month+day+second intervals are not representable)
  ,TEMPORAL_OUT_OF_RANGE        -- 'FAIL' (recommended; IMPORT fails on a date/timestamp outside 0001..9999), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min/max)
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
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'BASE64' end
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' and decof ~= 'VARCHAR' then decof = 'CAP' end
ivmode = string.upper(tostring(INTERVAL_HANDLING))
if ivmode ~= 'INTERVAL' then ivmode = 'VARCHAR' end
oormode = string.upper(tostring(TEMPORAL_OUT_OF_RANGE))
if oormode ~= 'NULL' and oormode ~= 'CLAMP' then oormode = 'FAIL' end
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
-- target schema name expression (TARGET_SCHEMA override or the source schema)
if TARGET_SCHEMA == null then tschema = [["schema_name"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
-- foreign-key parent schema: when TARGET_SCHEMA is set every table lands there, otherwise the parent keeps its own schema
if TARGET_SCHEMA == null then ref_sname_e = U('"ref_schema"') else ref_sname_e = sname_e end

-- Always-excluded schemas (system + temp). User schemas (incl. public) are kept; extension-owned tables are
-- excluded in the metadata query (pg_depend deptype 'e').
sys_filter = [[ and n.nspname not in (''pg_catalog'',''information_schema'') and n.nspname not like ''pg_toast%'' and n.nspname not like ''pg_temp%'' and n.nspname not like ''pg_%'' ]]
flt = [[ and n.nspname like '']]..SCHEMA_FILTER..[['' and c.relname like '']]..TABLE_FILTER..[['' ]]

-------------------------------------------------------------------------------------------------------
-- Remote (PostgreSQL) metadata queries. Inner literals are quote-doubled (embedded in statement '...').
-- Domains are resolved to their base type; len/precision/scale via the information_schema helper functions.
-------------------------------------------------------------------------------------------------------
-- Domains (incl. nested domains over domains) are resolved to their ultimate base predefined type via a
-- recursive walk of pg_type.typbasetype; dr.base_oid/dr.typname/dr.typcategory/dr.dmod describe that base.
eff_oid  = [[coalesce(dr.base_oid, a.atttypid)]]
eff_mod  = [[(case when dr.base_oid is not null then dr.dmod else a.atttypmod end)]]
columns_q = [[with recursive dr_walk(domain_oid, base_oid, dmod) as (
		select oid, typbasetype, typtypmod from pg_type where typtype = ''d''
		union all
		select w.domain_oid, b.typbasetype, case when w.dmod <> -1 then w.dmod else b.typtypmod end from dr_walk w join pg_type b on b.oid = w.base_oid where b.typtype = ''d''
	),
	dr_base as (select w.domain_oid, w.base_oid, w.dmod, bt.typname, bt.typcategory from dr_walk w join pg_type bt on bt.oid = w.base_oid where bt.typtype <> ''d'')
	select n.nspname as schema_name, c.relname as table_name, a.attname as column_name, a.attnum as ordinal_position,
	coalesce(dr.typname, t.typname) as type_name,
	coalesce(dr.typcategory, t.typcategory) as type_cat,
	t.typtype as type_type,
	information_schema._pg_char_max_length(]]..eff_oid..[[, ]]..eff_mod..[[) as char_len,
	information_schema._pg_numeric_precision(]]..eff_oid..[[, ]]..eff_mod..[[) as num_prec,
	information_schema._pg_numeric_scale(]]..eff_oid..[[, ]]..eff_mod..[[) as num_scale,
	information_schema._pg_datetime_precision(]]..eff_oid..[[, ]]..eff_mod..[[) as dt_prec,
	(case when a.attnotnull then 1 else 0 end) as not_null, a.attidentity as identity_type, a.attgenerated as generated_type,
	pg_get_expr(ad.adbin, ad.adrelid) as default_value
	from pg_attribute a join pg_class c on c.oid = a.attrelid join pg_namespace n on n.oid = c.relnamespace
	join pg_type t on t.oid = a.atttypid
	left join dr_base dr on dr.domain_oid = a.atttypid
	left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
	where a.attnum > 0 and not a.attisdropped and c.relkind in (''r'',''p'') and not c.relispartition
	and not exists (select 1 from pg_depend d where d.objid = c.oid and d.deptype = ''e'')]]..sys_filter..flt

pk_q = [[select n.nspname as schema_name, c.relname as table_name, att.attname as column_name, k.ord as column_position
	from pg_constraint con join pg_class c on c.oid = con.conrelid join pg_namespace n on n.oid = c.relnamespace
	join unnest(con.conkey) with ordinality k(attnum,ord) on true
	join pg_attribute att on att.attrelid = con.conrelid and att.attnum = k.attnum
	where con.contype = ''p'' and not c.relispartition]]..sys_filter..flt

fk_q = [[select n.nspname as schema_name, c.relname as table_name, con.conname as fk_name, ca.attname as fk_column,
	fn.nspname as ref_schema, fc.relname as ref_table, fa.attname as ref_column, k.ord as col_position
	from pg_constraint con join pg_class c on c.oid = con.conrelid join pg_namespace n on n.oid = c.relnamespace
	join pg_class fc on fc.oid = con.confrelid join pg_namespace fn on fn.oid = fc.relnamespace
	join unnest(con.conkey) with ordinality k(attnum,ord) on true
	join pg_attribute ca on ca.attrelid = con.conrelid and ca.attnum = k.attnum
	join unnest(con.confkey) with ordinality fk(attnum,ord) on fk.ord = k.ord
	join pg_attribute fa on fa.attrelid = con.confrelid and fa.attnum = fk.attnum
	where con.contype = ''f'' and not c.relispartition]]..sys_filter..flt

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

if ivmode == 'INTERVAL' then iv_t = [['INTERVAL DAY (9) TO SECOND(6)']] else iv_t = [['VARCHAR(100) ASCII']] end

-- Exasol column type, mapped by pg_type category (type_cat) with type_name overrides. Catch-all -> VARCHAR.
col_t = [[case
	when "type_name" = 'int2' then 'DECIMAL(5,0)'
	when "type_name" = 'int4' then 'DECIMAL(10,0)'
	when "type_name" = 'int8' then 'DECIMAL(19,0)'
	when "type_name" = 'oid' then 'DECIMAL(10,0)'
	when "type_name" = 'numeric' then ]]..dec_t..[[
	when "type_name" in ('float4','float8') then 'DOUBLE'
	when "type_name" = 'money' then 'DECIMAL(20,2)'
	when "type_name" = 'bool' then 'BOOLEAN'
	when "type_name" = 'bpchar' then case when "char_len" is null or "char_len" > 2000 then 'VARCHAR(2000000) UTF8' else 'CHAR(' || "char_len" || ') UTF8' end
	when "type_name" = 'char' then 'CHAR(1) UTF8'
	when "type_name" = 'varchar' then 'VARCHAR(' || (case when "char_len" is null or "char_len" > 2000000 then 2000000 else "char_len" end) || ') UTF8'
	when "type_name" = 'name' then 'VARCHAR(128) UTF8'
	when "type_name" = 'text' then 'VARCHAR(2000000) UTF8'
	when "type_name" = 'date' then 'DATE'
	when "type_name" = 'time' then 'VARCHAR(15) ASCII'
	when "type_name" = 'timetz' then 'VARCHAR(21) ASCII'
	when "type_name" = 'timestamp' then 'TIMESTAMP(' || (case when "dt_prec" between 0 and 9 then "dt_prec" else 6 end) || ')'
	when "type_name" = 'timestamptz' then 'TIMESTAMP(' || (case when "dt_prec" between 0 and 9 then "dt_prec" else 6 end) || ') WITH LOCAL TIME ZONE'
	when "type_name" = 'interval' then ]]..iv_t..[[
	when "type_name" = 'bytea' then 'VARCHAR(2000000) ASCII'
	when "type_name" = 'uuid' then 'CHAR(36) ASCII'
	when "type_cat" = 'I' or "type_name" = 'macaddr' then 'VARCHAR(45) ASCII'
	when "type_cat" = 'V' then 'VARCHAR(' || (case when "char_len" is null or "char_len" > 2000000 then 2000000 else "char_len" end) || ') ASCII'
	else 'VARCHAR(2000000) UTF8'
end]]

-- Unsupported = pseudo-types only (everything else maps, at worst to VARCHAR via ::text).
unsup = [[("type_cat" = 'P' or "type_name" is null)]]

-- DEFAULT mapping (literals + now()/current_timestamp/current_date; nextval/serial and generated skipped).
default_e = [[case
	when "default_value" is null then ''
	when "generated_type" = 's' then ''
	when "default_value" like 'nextval(%' then ''
	when upper("default_value") in ('NOW()','CURRENT_TIMESTAMP','TRANSACTION_TIMESTAMP()','STATEMENT_TIMESTAMP()','CLOCK_TIMESTAMP()','LOCALTIMESTAMP') then ' DEFAULT CURRENT_TIMESTAMP'
	when upper("default_value") in ('CURRENT_DATE') then ' DEFAULT CURRENT_DATE'
	when "default_value" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}$' then ' DEFAULT ' || "default_value"
	when upper("default_value") in ('TRUE','FALSE') then ' DEFAULT ' || upper("default_value")
	when "default_value" REGEXP_LIKE '^''.*''(::.*){0,1}$' then ' DEFAULT ' || regexp_replace("default_value", '^(''.*'')(::.*){0,1}$', '\1')
	else ''
end]]
coldef = [['"' || "exa_col" || '" ' || (]]..col_t..[[) || (]]..default_e..[[) || (case when "not_null" = 1 then ' NOT NULL' else '' end)]]

-- temporal source expressions (apply TEMPORAL_OUT_OF_RANGE; timestamptz normalized to the UTC instant)
if oormode == 'NULL' then
	date_src = [['case when ' || "col_q" || ' < date ''''0001-01-01'''' or ' || "col_q" || ' > date ''''9999-12-31'''' then null else ' || "col_q" || ' end']]
	ts_src   = [['case when ' || "col_q" || ' < timestamp ''''0001-01-01 00:00:00'''' or ' || "col_q" || ' > timestamp ''''9999-12-31 23:59:59.999999'''' then null else ' || "col_q" || ' end']]
	tstz_src = [['case when (' || "col_q" || ' at time zone ''''UTC'''') < timestamp ''''0001-01-01 00:00:00'''' or (' || "col_q" || ' at time zone ''''UTC'''') > timestamp ''''9999-12-31 23:59:59.999999'''' then null else (' || "col_q" || ' at time zone ''''UTC'''') end']]
elseif oormode == 'CLAMP' then
	date_src = [['case when ' || "col_q" || ' < date ''''0001-01-01'''' then date ''''0001-01-01'''' when ' || "col_q" || ' > date ''''9999-12-31'''' then date ''''9999-12-31'''' else ' || "col_q" || ' end']]
	ts_src   = [['case when ' || "col_q" || ' < timestamp ''''0001-01-01 00:00:00'''' then timestamp ''''0001-01-01 00:00:00'''' when ' || "col_q" || ' > timestamp ''''9999-12-31 23:59:59.999999'''' then timestamp ''''9999-12-31 23:59:59.999999'''' else ' || "col_q" || ' end']]
	tstz_src = [['case when (' || "col_q" || ' at time zone ''''UTC'''') < timestamp ''''0001-01-01 00:00:00'''' then timestamp ''''0001-01-01 00:00:00'''' when (' || "col_q" || ' at time zone ''''UTC'''') > timestamp ''''9999-12-31 23:59:59.999999'''' then timestamp ''''9999-12-31 23:59:59.999999'''' else (' || "col_q" || ' at time zone ''''UTC'''') end']]
else
	date_src = [["col_q"]]
	ts_src   = [["col_q"]]
	tstz_src = [['(' || "col_q" || ' at time zone ''''UTC'''')']]
end

if binmode == 'SKIP' then bin_imp = [['cast(null as varchar(10))']] else bin_imp = [['encode(' || "col_q" || ', ''''base64'''')']] end
if decof == 'VARCHAR' then num_imp = [["col_q" || '::text']] else num_imp = [["col_q"]] end
if trunc then text_imp = [['left(' || "col_q" || '::text, 2000000)']] else text_imp = [["col_q" || '::text']] end
-- interval: VARCHAR (lossless text) or native Exasol INTERVAL DAY TO SECOND (best-effort; pure day-time only,
-- month/year-containing intervals are passed as text and fail the IMPORT loudly - use 'VARCHAR' for those).
if ivmode == 'INTERVAL' then bin_iv = [['case when extract(year from ' || "col_q" || ') = 0 and extract(month from ' || "col_q" || ') = 0 then extract(day from justify_hours(' || "col_q" || '))::text || '''' '''' || to_char(justify_hours(' || "col_q" || '), ''''HH24:MI:SS.US'''') else ' || "col_q" || '::text end']] else bin_iv = [["col_q" || '::text']] end

-- source SELECT expression(s) for the IMPORT (must align positionally with coldef).
src = [[case
	when "type_name" = 'money' then "col_q" || '::numeric'
	when "type_name" = 'bytea' then ]]..bin_imp..[[
	when "type_name" = 'date' then ]]..date_src..[[
	when "type_name" = 'timestamp' then ]]..ts_src..[[
	when "type_name" = 'timestamptz' then ]]..tstz_src..[[
	when "type_name" = 'interval' then ]]..bin_iv..[[
	when "type_name" in ('time','timetz','uuid') then "col_q" || '::text'
	when "type_name" = 'numeric' then ]]..num_imp..[[
	when "type_name" in ('int2','int4','int8','oid','float4','float8','bool','bpchar','varchar','name') then "col_q"
	when "type_name" = 'text' then ]]..text_imp..[[
	when "type_cat" in ('U','A','R','G','E','V','I') or "type_type" = 'c' then ]]..text_imp..[[
	else "col_q" || '::text'
end]]

-- constraint-state word + trailing comment (final CONSTRAINT STATE section uses MODIFY CONSTRAINT).
if cstate == 'FORCE_ENABLE' then sw = 'enable'; scomment = [[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; scomment = [[  -- matches PostgreSQL source (keys active)]]
else sw = 'disable'; scomment = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]
fkname = [[coalesce(nullif("fk_name",''), "table_name" || '_FK_' || "ref_table")]]

-- optional CTEs --------------------------------------------------------------------------------------
comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select n.nspname as schema_name, c.relname as table_name, d.objsubid as sub, a.attname as column_name, d.description as comment_text from pg_description d join pg_class c on c.oid = d.objoid join pg_namespace n on n.oid = c.relnamespace left join pg_attribute a on a.attrelid = d.objoid and a.attnum = d.objsubid where c.relkind in (''r'',''p'') and not c.relispartition]]..sys_filter..flt..[[') )
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" = 0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" > 0)]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select n.nspname as schema_name, c.relname as view_name, pg_get_viewdef(c.oid, true) as view_def from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in (''v'',''m'')]]..sys_filter..flt..[[') )
,vv_views as (select '-- ' || "schema_name" || '.' || "view_name" || '  (PostgreSQL view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (PostgreSQL SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

part_cte = ''  part_union = ''
if gen_part then
	part_cte = [[
,vv_part_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select n.nspname as schema_name, c.relname as table_name, pg_get_partkeydef(c.oid) as part_def from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind = ''p'']]..sys_filter..flt..[[') )
,vv_partcol_raw as (select "schema_name","table_name","part_def",
	case when instr("part_def",'(') > 0 and instr("part_def",',') = 0 then trim(substr("part_def", instr("part_def",'(')+1, instr("part_def",')') - instr("part_def",'(') - 1)) else null end as "raw_col"
	from vv_part_raw)
,vv_part as (
	select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" PARTITION BY "' || ]]..U('"raw_col"')..[[ || '";' as sql_text
	from vv_partcol_raw where "raw_col" is not null and "raw_col" REGEXP_LIKE '^[A-Za-z_][A-Za-z0-9_]*$'
	union all
	select '-- "' || ]]..U('"schema_name"')..[[ || '"."' || ]]..U('"table_name"')..[[ || '" PostgreSQL partitioning not auto-mapped (review and add PARTITION BY manually if appropriate): ' || "part_def" as sql_text
	from vv_partcol_raw where "raw_col" is null or not "raw_col" REGEXP_LIKE '^[A-Za-z_][A-Za-z0-9_]*$')]]
	part_union = "\n".. [[UNION ALL select 37, cast('-- ### PARTITION BY (best-effort from the PostgreSQL partition key; complex partitioning listed as a review note) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 38, sql_text from vv_part]]
end

-- CHECK_MIGRATION: per table a wide single-scan typed metrics row is computed on BOTH systems; a per-schema
-- summary unpivots+joins them, flagging each metric OK/DEVIATION. Metrics are cross-database comparable so
-- faithfully migrated data shows no false deviation: row/NULL/DISTINCT counts, numeric MIN/MAX/SUM (exact
-- decimals only), date/timestamp MIN/MAX, char length MIN/MAX. Binary/JSON/XML/array/geometric/composite get
-- NULL counts only.
check_cte = ''  check_union = ''
if gen_check then
	check_cte = [[
,vv_chk_cols as (select c.* from vv_columns c where not (]]..unsup..[[))
,vv_chk_base as (
	select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord",
	       sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || x."exa_col" || '"' when x."type_name" = 'oid' then x."col_q" || '::bigint' else x."col_q" end as "ref"  -- oid has no direct cast to numeric on PostgreSQL; ::bigint does
	from vv_chk_cols x
	cross join (select 'Exasol' as "db_system" union all select 'Postgres' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 8) mid
)
,vv_chk_expr as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","col_q","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide_name",
	       (case
	          when "metric_id" = 0 and "ordinal_position" = "min_ord" then 'cast(count(*) as decimal(36,0))'
	          when "metric_id" = 1 and "not_null" = 0 then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))'
	          when "metric_id" = 2 and "type_name" in ('int2','int4','int8','oid') then 'cast(min(' || "ref" || ') as decimal(20,0))'
	          when "metric_id" = 2 and "type_name" = 'money' then 'cast(min(' || "ref" || ') as decimal(20,2))'
	          when "metric_id" = 2 and "type_name" = 'numeric' and "num_prec" between 1 and 36 then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 2 and "type_name" in ('date','timestamp') then 'min(' || "ref" || ')'
	          when "metric_id" = 3 and "type_name" in ('int2','int4','int8','oid') then 'cast(max(' || "ref" || ') as decimal(20,0))'
	          when "metric_id" = 3 and "type_name" = 'money' then 'cast(max(' || "ref" || ') as decimal(20,2))'
	          when "metric_id" = 3 and "type_name" = 'numeric' and "num_prec" between 1 and 36 then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 3 and "type_name" in ('date','timestamp') then 'max(' || "ref" || ')'
	          when "metric_id" = 4 and not ("type_name" in ('bytea','json','jsonb','xml','tsvector','tsquery') or "type_cat" in ('A','G') or "type_type" = 'c') then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 5 and "type_name" in ('int2','int4','int8','oid') then 'cast(sum(' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 5 and "type_name" = 'money' then 'cast(sum(' || "ref" || ') as decimal(36,2))'
	          when "metric_id" = 5 and "type_name" = 'numeric' and "num_prec" between 1 and 36 then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))'
	          when "metric_id" = 6 and "type_name" in ('varchar','text','name') then 'cast(min(length(' || "ref" || ')) as decimal(36,0))'
	          when "metric_id" = 7 and "type_name" in ('varchar','text','name') then 'cast(max(length(' || "ref" || ')) as decimal(36,0))'
	        end) as "metric_expr"
	from vv_chk_base
)
,vv_chk_named as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","col_q","ordinal_position","db_system","metric_id","wide_name","metric_expr",
	       (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_DISTINCT' when 5 then "exa_col" || '_SUM' when 6 then "exa_col" || '_MINLEN' when 7 then "exa_col" || '_MAXLEN' end) as "metric_name"
	from vv_chk_expr where "metric_expr" is not null
)
,vv_chk_sys as (
	select "exa_schema","exa_table","schema_name","table_name","wide_name","db_system",
	       'select cast(' || '''' || "db_system" || '''' || ' as varchar(10)) as "DB_SYSTEM", ' || listagg("metric_expr" || ' as "' || "metric_name" || '"', ', ') within group (order by "ordinal_position","metric_id") || ' from ' || (case when "db_system" = 'Exasol' then '"' || "exa_schema" || '"."' || "exa_table" || '"' else '"' || "schema_name" || '"."' || "table_name" || '"' end) as "sys_select"
	from vv_chk_named group by "exa_schema","exa_table","schema_name","table_name","wide_name","db_system"
)
,vv_chk_wide as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "wide_name" || '" AS ' || max(case when "db_system" = 'Exasol' then "sys_select" end) || ' UNION ALL select * from (IMPORT FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || replace(max(case when "db_system" = 'Postgres' then "sys_select" end), '''', '''''') || '''' || ') ;' as sql_text
	from vv_chk_sys group by "exa_schema","wide_name"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system",
	       'select ' || '''' || "exa_table" || '''' || ' as "TABLE_NAME", ' || '''' || "metric_name" || '''' || ' as "METRIC", to_char("' || "metric_name" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide_name" || '" where "DB_SYSTEM" = ' || '''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'CREATE OR REPLACE TABLE "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" AS select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", t."VAL" as "POSTGRES_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(t."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || listagg(case when "db_system" = 'Exasol' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') e join (' || listagg(case when "db_system" = 'Postgres' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') t on e."TABLE_NAME" = t."TABLE_NAME" and e."METRIC" = t."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
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
	select ]]..sname_e..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table", ]]..U('"column_name"')..[[ as "exa_col", '"' || "column_name" || '"' as "col_q", t.*
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..columns_q..[[') t
)
,vv_unsupported as (
	select '-- !!! UNSUPPORTED TYPE - column NOT migrated: "' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" (PostgreSQL type: ' || coalesce("type_name",'null') || ') - migrate this column manually !!!' as sql_text
	from vv_columns where ]]..unsup..[[
)
,vv_pk_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..pk_q..[['))
,vv_pk as (
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" PRIMARY KEY (' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "column_position") || ') DISABLE;' as sql_text,
	       ]]..sname_e..[[ as "exa_schema", ]]..U('"table_name"')..[[ as "exa_table"
	from vv_pk_raw group by "schema_name","table_name"
)
,vv_fk_raw as (select f.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..fk_q..[[') f where exists (select 1 from vv_columns c where c."schema_name" = f."ref_schema" and c."table_name" = f."ref_table"))
,vv_fk as (
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U(fkname)..[[ || '" FOREIGN KEY (' || group_concat('"' || ]]..U('"fk_column"')..[[ || '"' order by "col_position") || ') REFERENCES "' || ]]..ref_sname_e..[[ || '"."' || ]]..U('"ref_table"')..[[ || '" (' || group_concat('"' || ]]..U('"ref_column"')..[[ || '"' order by "col_position") || ') DISABLE;' as sql_text
	from vv_fk_raw group by "schema_name","table_name","fk_name","ref_schema","ref_table"
)
,vv_create_schemas as (select distinct 'CREATE SCHEMA IF NOT EXISTS "' || "exa_schema" || '";' as sql_text from vv_columns)
,vv_create_tables as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "exa_table" || '" (' || group_concat(case when ]]..unsup..[[ then null else (]]..coldef..[[) end order by "ordinal_position" separator ', ') || ');' as sql_text
	from vv_columns group by "exa_schema","exa_table"
)
,vv_imports as (
	select 'IMPORT INTO "' || "exa_schema" || '"."' || "exa_table" || '" FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || 'select ' || group_concat(case when ]]..unsup..[[ then null else (]]..src..[[) end order by "ordinal_position" separator ', ') || ' from "' || "schema_name" || '"."' || "table_name" || '"' || '''' || ';' as sql_text
	from vv_columns group by "exa_schema","exa_table","schema_name","table_name"
)]]..comments_cte..views_cte..part_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_unsupported
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
--   * The PostgreSQL database must be reachable from this Exasol database.
--   * The credentials used in the connection must be valid.
--   * Use the latest PostgreSQL JDBC driver (postgresql).
--
-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * postgresql 42.7.11 or higher
--       https://mvnrepository.com/artifact/org.postgresql/postgresql
--   * Driver setup guide:
--       https://docs.exasol.com/db/latest/loading_data/connect_sources/postgresql.htm
--
-- Create a connection to the PostgreSQL database (adjust host, database name and credentials),
-- then run the accompanying test query.

CREATE OR REPLACE CONNECTION POSTGRESQL_JDBC
    TO 'jdbc:postgresql://postgresql_host_or_ip:5432/my_database'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT POSTGRESQL_JDBC STATEMENT 'SELECT ''Connection works''');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.POSTGRESQL_TO_EXASOL(
    'POSTGRESQL_JDBC',  -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes (PostgreSQL folds unquoted names to lower-case, so nothing is lost); false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'public', 'sales_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate PostgreSQL comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from the PostgreSQL partition key (single column); complex partitioning is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; bytea migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; numeric>36 -> DECIMAL(36,s), unconstrained -> DECIMAL(36,18); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'VARCHAR',          -- INTERVAL_HANDLING: 'VARCHAR' (recommended; interval as lossless text) or 'INTERVAL' (native Exasol INTERVAL, best-effort)
    'FAIL',             -- TEMPORAL_OUT_OF_RANGE: 'FAIL' (recommended; IMPORT fails on a date/timestamp outside 0001..9999), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min/max)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
