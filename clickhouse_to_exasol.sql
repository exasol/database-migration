create schema if not exists database_migration;

/*
    clickhouse_to_exasol.sql  -  generate the statements to migrate a ClickHouse database to Exasol v8.

    Source: ClickHouse (verified on ClickHouse 26.6). This script runs on the TARGET Exasol database, reads the
    SOURCE metadata through a JDBC connection (native system.columns / system.tables catalog) and RETURNS the
    statements (CREATE SCHEMA / CREATE TABLE incl. PRIMARY KEY / COMMENTs / IMPORT / a final CONSTRAINT STATE
    section / optional VIEW review section / optional DATA VALIDATION). It changes nothing itself - review the
    output and run it in the order returned. A ClickHouse "database" maps to an Exasol schema.

    DATA TYPE MAPPING (every ClickHouse type is covered; each CREATE-probed live on ClickHouse 26.6):
      Int8/Int16/Int32/Int64 -> DECIMAL(3/5/10/19,0); UInt8/UInt16/UInt32/UInt64 -> DECIMAL(3/5/10/20,0);
      Int128/Int256/UInt128/UInt256 -> DECIMAL(36,0) (need > 36 digits -> DECIMAL_OVERFLOW);
      Decimal(P,S) (incl. Decimal32/64/128/256) -> DECIMAL(P,S) (P>36 -> DECIMAL_OVERFLOW);
      Float32/Float64/BFloat16 -> DOUBLE; String -> VARCHAR(2000000) UTF8; FixedString(n) -> VARCHAR(n) UTF8;
      Date/Date32 -> DATE; DateTime -> TIMESTAMP(0); DateTime64(p) -> TIMESTAMP(min(p,9)) (full sub-second fidelity);
      Bool -> BOOLEAN; UUID -> CHAR(36); Enum8/Enum16 -> VARCHAR (the label); IPv4/IPv6 -> VARCHAR(45);
      Array/Tuple/Map/Nested/JSON/Variant/Dynamic and the geo types (Point/Ring/Polygon/MultiPolygon/LineString/
      MultiLineString) -> VARCHAR(2000000) holding ClickHouse's own text form. Nullable(T)/LowCardinality(T) are
      unwrapped to T. SimpleAggregateFunction(f,T) -> the mapping of T. Anything unexpected -> VARCHAR(2000000)
      catch-all (flagged with a NOTE). Hard limits (fail loudly, never corrupt): a value needing > 36 digits under
      DECIMAL_OVERFLOW='CAP'; a String/FixedString value > 2,000,000 characters (unless TRUNCATE_LONG_STRINGS=true).

    WHY SOME COLUMNS ARE READ WITH toString() ON THE SOURCE (verified live with the ClickHouse JDBC driver): the
    driver cannot transfer some types directly ("JDBC type unknown"), so the generated IMPORT reads them as text -
      UUID, Array, Tuple, Map, Nested, IPv4, IPv6, JSON, Variant, Dynamic and every geo type -> toString(..);
      Int128/256, UInt128/256 and Decimal(P>36) under DECIMAL_OVERFLOW='VARCHAR' -> toString(..) (lossless text).
      Everything else (Int/UInt that fit, Float, fitting Decimal, String/FixedString, Date/Date32, DateTime,
      DateTime64 with full sub-seconds, Bool, Enum -> its label) transfers directly.

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are not
    affected by differing source/target locale settings. Character data is stored as UTF8. DateTime / DateTime64
    hold a UTC instant in ClickHouse; they are migrated as the transferred wall-clock TIMESTAMP.

    VALUE CONVERSIONS forced by Exasol's data model (documented, so they never fail silently): Exasol stores an
    empty string as NULL, so a ClickHouse String '' becomes NULL; Exasol has no Float inf/nan, so a ClickHouse
    Float inf/-inf/nan becomes NULL (read via if(isFinite(..))). Because of these, NOT NULL is emitted only on
    exact numeric / temporal / boolean columns, never on character or Float columns. CHECK_MIGRATION accounts for
    both (it counts '' and inf/nan as NULL on the ClickHouse side) so a faithful migration reports no deviation.

    CONSTRAINTS: ClickHouse has NO foreign keys and its PRIMARY KEY / ORDER BY is a NON-UNIQUE, NON-ENFORCED sort
    key. The sort-key columns are migrated as an Exasol PRIMARY KEY created DISABLED (useful optimizer/BI metadata);
    a final CONSTRAINT STATE section sets it per CONSTRAINT_STATE (run after the IMPORTs). CONSTRAINT_STATE=
    'FORCE_ENABLE' may fail if the source data contains duplicate key values (ClickHouse does not prevent them).

    Only real data tables are migrated (MergeTree family, Memory, Log, ...). Views (View/MaterializedView) are
    emitted as a commented review section (GENERATE_VIEWS). Integration/virtual-engine tables (Distributed,
    Dictionary, Kafka, S3Queue, MySQL, PostgreSQL, MongoDB, ...) are skipped with a NOTE (they reference external or
    other tables). ALIAS / EPHEMERAL columns are not stored and are skipped with a NOTE.

    Not migrated (out of scope): indexes, projections, TTL rules, ClickHouse partitioning/sharding (physical/
    distribution-oriented; no value-based Exasol equivalent), row policies, dictionaries, materialized-view logic.

    Always excluded (only real user data): the ClickHouse system databases system, information_schema and
    INFORMATION_SCHEMA.
*/
--/
create or replace script database_migration.CLICKHOUSE_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol (to the ClickHouse source) -> e.g. CLICKHOUSE_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted (ClickHouse identifiers are case-sensitive - use false if names differ only by case)
  ,SCHEMA_FILTER                -- filter for the source ClickHouse databases (system databases always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source ClickHouse database name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; the PRIMARY KEY (from the sort key) is always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate ClickHouse table/column comments as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views (View/MaterializedView) as a commented manual-review section
  ,DECIMAL_OVERFLOW             -- 'CAP' (recommended; > 36 digits -> DECIMAL(36,s)), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text) for Int128/256, UInt128/256 and Decimal with precision > 36
  ,TRUNCATE_LONG_STRINGS        -- true: String/FixedString values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
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
trunc    = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
if TARGET_SCHEMA == null or TARGET_SCHEMA == '' then tschema = [["database"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)

-- ClickHouse filter (system databases always excluded). One quoting level here (inner literals doubled).
function chflt(db, tn)
	return [[ and ]]..db..[[ not in (''system'',''information_schema'',''INFORMATION_SCHEMA'') and ]]..db..[[ like '']]..SCHEMA_FILTER..[['' and ]]..tn..[[ like '']]..TABLE_FILTER..[['' ]]
end
-- engine classification (used to migrate only real data tables; emit views commented; skip integration engines)
data_eng = [[(t.engine like ''%MergeTree%'' or t.engine in (''Memory'',''Log'',''TinyLog'',''StripeLog'',''Buffer'',''EmbeddedRocksDB'',''KeeperMap'',''File''))]]
view_eng = [[engine in (''View'',''MaterializedView'',''LiveView'',''WindowView'')]]

-------------------------------------------------------------------------------------------------------
-- Remote (ClickHouse) metadata queries (native system.* catalog). Inner literals are quote-doubled.
-------------------------------------------------------------------------------------------------------
columns_q = [[select c.database, c.table, c.name, c.position, c.type, c.numeric_precision, c.numeric_scale, c.datetime_precision, c.character_octet_length, c.is_in_primary_key, c.default_kind, c.default_expression from system.columns c inner join system.tables t on t.database = c.database and t.name = c.table where ]]..data_eng..chflt('c.database','c.table')

pk_q = [[select c.database, c.table, c.name, c.position from system.columns c inner join system.tables t on t.database = c.database and t.name = c.table where c.is_in_primary_key = 1 and ]]..data_eng..chflt('c.database','c.table')

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions producing the generated statement text.
-------------------------------------------------------------------------------------------------------
-- unwrap Nullable()/LowCardinality() then take the leading identifier -> base type name; resolve
-- SimpleAggregateFunction(f,T) to T's base name. (\1 backreference; \( \) literal parens - DbVis-clean.)
base_e = [[regexp_substr(regexp_replace(regexp_replace("type", '^LowCardinality\((.*)\)$', '\1'), '^Nullable\((.*)\)$', '\1'), '^[A-Za-z][A-Za-z0-9]*')]]
saf_e  = [[regexp_substr(regexp_replace("type", '^SimpleAggregateFunction\([^,]*, (.*)\)$', '\1'), '^[A-Za-z][A-Za-z0-9]*')]]

sc = [[(case when "nscale" is null or "nscale" < 0 then 0 when "nscale" > 36 then 36 else "nscale" end)]]
if decof == 'DOUBLE' then int_over_t = [['DOUBLE']] elseif decof == 'VARCHAR' then int_over_t = [['VARCHAR(80) ASCII']] else int_over_t = [['DECIMAL(36,0)']] end
if decof == 'DOUBLE' then
	dec_fixed = [[case when "nprec" > 36 then 'DOUBLE' else 'DECIMAL(' || "nprec" || ',' || ]]..sc..[[ || ')' end]]
elseif decof == 'VARCHAR' then
	dec_fixed = [[case when "nprec" > 36 then 'VARCHAR(80) ASCII' else 'DECIMAL(' || "nprec" || ',' || ]]..sc..[[ || ')' end]]
else
	dec_fixed = [[case when "nprec" > 36 then 'DECIMAL(36,' || ]]..sc..[[ || ')' else 'DECIMAL(' || "nprec" || ',' || ]]..sc..[[ || ')' end]]
end

col_t = [[case
	when "dtype" = 'Int8' then 'DECIMAL(3,0)'
	when "dtype" = 'Int16' then 'DECIMAL(5,0)'
	when "dtype" = 'Int32' then 'DECIMAL(10,0)'
	when "dtype" = 'Int64' then 'DECIMAL(19,0)'
	when "dtype" = 'UInt8' then 'DECIMAL(3,0)'
	when "dtype" = 'UInt16' then 'DECIMAL(5,0)'
	when "dtype" = 'UInt32' then 'DECIMAL(10,0)'
	when "dtype" = 'UInt64' then 'DECIMAL(20,0)'
	when "dtype" in ('Int128','Int256','UInt128','UInt256') then ]]..int_over_t..[[
	when "dtype" in ('Float32','Float64','BFloat16') then 'DOUBLE'
	when "dtype" = 'Decimal' then ]]..dec_fixed..[[
	when "dtype" = 'String' then 'VARCHAR(2000000) UTF8'
	when "dtype" = 'FixedString' then 'VARCHAR(' || (case when "coct" is null or "coct" > 2000000 then 2000000 else "coct" end) || ') UTF8'
	when "dtype" in ('Date','Date32') then 'DATE'
	when "dtype" = 'DateTime' then 'TIMESTAMP(0)'
	when "dtype" = 'DateTime64' then 'TIMESTAMP(' || (case when "dtprec" is null then 3 when "dtprec" > 9 then 9 else "dtprec" end) || ')'
	when "dtype" = 'Bool' then 'BOOLEAN'
	when "dtype" = 'UUID' then 'CHAR(36) ASCII'
	when "dtype" in ('Enum8','Enum16') then 'VARCHAR(2000000) UTF8'
	when "dtype" in ('IPv4','IPv6') then 'VARCHAR(45) ASCII'
	else 'VARCHAR(2000000) UTF8'
end]]

-- DEFAULT mapping (only literal / now() defaults; ClickHouse expression defaults are skipped).
default_e = [[case
	when "default_kind" <> 'DEFAULT' or "default_expr" is null or "default_expr" = '' then ''
	when upper("default_expr") in ('NOW()','CURRENT_TIMESTAMP') then ' DEFAULT CURRENT_TIMESTAMP'
	when upper("default_expr") in ('TODAY()','CURRENT_DATE') then ' DEFAULT CURRENT_DATE'
	when upper("default_expr") in ('TRUE','FALSE') then ' DEFAULT ' || upper("default_expr")
	when "default_expr" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}$' then ' DEFAULT ' || "default_expr"
	when "default_expr" REGEXP_LIKE '^''[A-Za-z0-9 _.,:@/-]*''$' then ' DEFAULT ' || "default_expr"
	else ''
end]]
coldef = [['"' || "exa_col" || '" ' || (]]..col_t..[[) || (]]..default_e..[[) || (case when "not_null" = 1 then ' NOT NULL' else '' end)]]

-- source SELECT expression for the IMPORT (must align positionally with coldef).
if trunc then txt_src = [['substringUTF8("' || "name" || '", 1, 2000000)']] else txt_src = [['"' || "name" || '"']] end
src = [[case
	when "dtype" = 'AggregateFunction' then 'cast(null as Nullable(String))'
	when "is_saf" then 'toString("' || "name" || '")'
	when "dtype" in ('UUID','Array','Tuple','Map','Nested','IPv4','IPv6','JSON','Variant','Dynamic','Point','Ring','Polygon','MultiPolygon','LineString','MultiLineString') then 'toString("' || "name" || '")'
	when "dtype" in ('Int128','Int256','UInt128','UInt256') and ']]..decof..[[' = 'VARCHAR' then 'toString("' || "name" || '")'
	when "dtype" = 'Decimal' and ']]..decof..[[' = 'VARCHAR' and "nprec" > 36 then 'toString("' || "name" || '")'
	when "dtype" in ('Float32','Float64','BFloat16') then 'if(isFinite("' || "name" || '"), "' || "name" || '", NULL)'
	when "dtype" in ('String','FixedString') then ]]..txt_src..[[
	else '"' || "name" || '"'
end]]

if cstate == 'FORCE_ENABLE' then sw = 'enable'; scomment = [[  -- forced ENABLE (Exasol re-validates the data; may fail on duplicate sort-key values)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; scomment = [[  -- keys active (Exasol re-validates; ClickHouse sort key is not unique -> may fail on duplicates)]]
else sw = 'disable'; scomment = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table"')..[[ || '"']]
known = [["dtype" in ('Int8','Int16','Int32','Int64','Int128','Int256','UInt8','UInt16','UInt32','UInt64','UInt128','UInt256','Float32','Float64','BFloat16','Decimal','String','FixedString','Date','Date32','DateTime','DateTime64','Bool','UUID','Enum8','Enum16','Array','Tuple','Map','IPv4','IPv6','JSON','Point','Ring','Polygon','MultiPolygon','LineString','MultiLineString','Variant','Dynamic','AggregateFunction')]]

-- optional CTEs --------------------------------------------------------------------------------------
comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_comments_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select t.database, t.name, 0 as sub, cast(null as Nullable(String)) as col, t.comment from system.tables t where t.comment <> '''' and ]]..data_eng..chflt('t.database','t.name')..[[ union all select c.database, c.table, c.position as sub, c.name as col, c.comment from system.columns c inner join system.tables t on t.database = c.database and t.name = c.table where c.comment <> '''' and ]]..data_eng..chflt('c.database','c.table')..[[') cc ("database","table","sub","column_name","comment_text"))
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" = 0)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("comment_text", '''', '''''') || '''' || ';' as sql_text from vv_comments_raw where "sub" > 0)]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select database, name, create_table_query from system.tables t where ]]..view_eng..chflt('t.database','t.name')..[[') v ("database","view_name","view_def"))
,vv_views as (select '-- ' || "database" || '.' || "view_name" || '  - ClickHouse view, review and adapt to Exasol SQL manually' || chr(10) || '-- ' || replace("view_def", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (ClickHouse SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

-- CHECK_MIGRATION: per table a wide single-scan typed metrics row on BOTH systems; a per-schema summary unpivots+
-- joins them, flagging each metric OK/DEVIATION. Mapping-aware: exact integer/fixed-decimal(<=36) MIN/MAX/SUM only
-- (NOT Float, huge integers or Decimal>36), date/timestamp MIN/MAX as text to the second, no complex/aggregate
-- value metrics. Numeric metrics stored typed (decimal) then TO_CHAR'd in the summary to avoid text-format false
-- deviations. Source metric SELECT embedded via IMPORT (quotes doubled by REPLACE); ClickHouse dates via
-- formatDateTime, Exasol dates via to_char (same canonical text).
check_cte = ''  check_union = ''
if gen_check then
	chk_num  = [["dtype" in ('Int8','Int16','Int32','Int64','UInt8','UInt16','UInt32','UInt64') or ("dtype" = 'Decimal' and "nprec" between 1 and 36)]]
	chk_dt   = [["dtype" in ('Date','Date32','DateTime','DateTime64')]]
	dist_ok  = [["dtype" in ('Int8','Int16','Int32','Int64','UInt8','UInt16','UInt32','UInt64','String','FixedString','Date','Date32','DateTime','DateTime64','Bool','UUID','Enum8','Enum16','IPv4','IPv6') or ("dtype" = 'Decimal' and "nprec" between 1 and 36)]]
	check_cte = [[
,vv_chk_cols as (select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord" from vv_columns x where (]]..known..[[) and "dtype" <> 'AggregateFunction' and not "skip_col")
,vv_chk_x as (
	select c.*, sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || c."exa_col" || '"' else '"' || c."name" || '"' end as "ref"
	from vv_chk_cols c
	cross join (select 'Exasol' as "db_system" union all select 'CLICKHOUSE' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 6) mid
)
,vv_chk_e as (
	select "exa_schema","exa_table","database","table","exa_col","name","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide",
	   (case
	      when "metric_id" = 0 and "ordinal_position" = "min_ord" then 'cast(count(*) as decimal(36,0))'
	      -- NULL count. On ClickHouse a String '' and a Float inf/nan both become NULL in Exasol, so count them as NULL too.
	      when "metric_id" = 1 and "not_null" = 0 and "dtype" = 'String' and "db_system" = 'CLICKHOUSE' then 'cast(count(case when ' || "ref" || ' is null or length(' || "ref" || ') = 0 then 1 end) as decimal(36,0))'
	      when "metric_id" = 1 and "not_null" = 0 and "dtype" in ('Float32','Float64','BFloat16') and "db_system" = 'CLICKHOUSE' then 'cast(count(case when ' || "ref" || ' is null or not isFinite(' || "ref" || ') then 1 end) as decimal(36,0))'
	      when "metric_id" = 1 and "not_null" = 0 then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))'
	      -- MIN/MAX/SUM (exact numerics). ClickHouse uses the -OrNull combinator so an empty table yields NULL like Exasol
	      -- (not the type default 0); SUM casts to Decimal first so a ClickHouse integer sum cannot silently overflow/wrap.
	      when "metric_id" = 2 and (]]..chk_num..[[) then (case when "db_system" = 'Exasol' then 'cast(min(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'cast(minOrNull(' || "ref" || ') as Nullable(Decimal(36,' || ]]..sc..[[ || ')))' end)
	      when "metric_id" = 2 and (]]..chk_dt..[[) then (case when "db_system" = 'Exasol' then 'to_char(min(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')' else 'formatDateTime(minOrNull(' || "ref" || '),''%Y-%m-%d %H:%i:%S'')' end)
	      when "metric_id" = 3 and (]]..chk_num..[[) then (case when "db_system" = 'Exasol' then 'cast(max(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'cast(maxOrNull(' || "ref" || ') as Nullable(Decimal(36,' || ]]..sc..[[ || ')))' end)
	      when "metric_id" = 3 and (]]..chk_dt..[[) then (case when "db_system" = 'Exasol' then 'to_char(max(' || "ref" || '),''YYYY-MM-DD HH24:MI:SS'')' else 'formatDateTime(maxOrNull(' || "ref" || '),''%Y-%m-%d %H:%i:%S'')' end)
	      when "metric_id" = 4 and (]]..chk_num..[[) then (case when "db_system" = 'Exasol' then 'cast(sum(' || "ref" || ') as decimal(36,' || ]]..sc..[[ || '))' else 'cast(sumOrNull(cast(' || "ref" || ' as Nullable(Decimal(38,' || ]]..sc..[[ || ')))) as Nullable(Decimal(36,' || ]]..sc..[[ || ')))' end)
	      -- DISTINCT count. On ClickHouse map '' -> NULL so it is excluded like on Exasol (length()=0, no quoting needed).
	      when "metric_id" = 5 and (]]..dist_ok..[[) and "dtype" = 'String' and "db_system" = 'CLICKHOUSE' then 'cast(count(distinct case when length(' || "ref" || ') = 0 then null else ' || "ref" || ' end) as decimal(36,0))'
	      when "metric_id" = 5 and (]]..dist_ok..[[) then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))'
	    end) as "mexpr",
	   (case "metric_id" when 0 then 'ROW_CNT' when 1 then "exa_col" || '_NULLS' when 2 then "exa_col" || '_MIN' when 3 then "exa_col" || '_MAX' when 4 then "exa_col" || '_SUM' when 5 then "exa_col" || '_DISTINCT' end) as "mname"
	from vv_chk_x
)
,vv_chk_named as (select * from vv_chk_e where "mexpr" is not null)
,vv_chk_sys as (
	select "exa_schema","exa_table","database","table","wide","db_system",
	   case when "db_system" = 'Exasol'
	     then 'select ''Exasol'' as "DB_SYSTEM", ' || group_concat("mexpr" || ' as "' || "mname" || '"' order by "ordinal_position","metric_id" separator ', ') || ' from "' || "exa_schema" || '"."' || "exa_table" || '"'
	     else 'select ''CLICKHOUSE'' as "DB_SYSTEM", x.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ' || '''' || replace('select ' || group_concat("mexpr" order by "ordinal_position","metric_id" separator ', ') || ' from "' || "database" || '"."' || "table" || '"', '''', '''''') || '''' || ') x'
	   end as "sel"
	from vv_chk_named group by "exa_schema","exa_table","database","table","wide","db_system"
)
,vv_chk_wide as (
	select 'create or replace table "' || "exa_schema" || '"."' || "wide" || '" as ' || max(case when "db_system" = 'Exasol' then "sel" end) || ' UNION ALL ' || max(case when "db_system" = 'CLICKHOUSE' then "sel" end) || ';' as sql_text
	from vv_chk_sys group by "exa_schema","wide"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system","wide","mname",
	   'select ''' || "exa_table" || ''' as "TABLE_NAME", ''' || "mname" || ''' as "METRIC", to_char("' || "mname" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide" || '" where "DB_SYSTEM" = ''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'create or replace table "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" as select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", h."VAL" as "CLICKHOUSE_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(h."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || group_concat(case when "db_system" = 'Exasol' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') e join (' || group_concat(case when "db_system" = 'CLICKHOUSE' then "frag" end order by "exa_table","ordinal_position","metric_id" separator ' union all ') || ') h on e."TABLE_NAME" = h."TABLE_NAME" and e."METRIC" = h."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
	from vv_chk_unpiv group by "exa_schema"
)]]
	check_union = "\n".. [[UNION ALL select 70, cast('-- ### DATA VALIDATION (CHECK_MIGRATION) - run AFTER the IMPORTs; compares source vs target metrics ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 71, sql_text from vv_chk_wide
UNION ALL select 72, cast('-- per-schema validation summary - one row per metric, STATUS = OK / DEVIATION' as varchar(2000000))
UNION ALL select 73, sql_text from vv_chk_summary
UNION ALL select 74, cast('-- review deviations with:  select * from "DATABASE_MIGRATION"."<schema>_MIG_CHK" where "STATUS" = ''DEVIATION'';' as varchar(2000000))]]
end

suc, res = pquery([[
with vv_pre as (
	select t.*, ]]..base_e..[[ as "base", ]]..saf_e..[[ as "saf"
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..columns_q..[[') t ("database","table","name","position","type","nprec","nscale","dtprec","coct","is_pk","default_kind","default_expr")
)
,vv_columns as (
	select ]]..sname_e..[[ as "exa_schema", ]]..U('"table"')..[[ as "exa_table", ]]..U('"name"')..[[ as "exa_col",
	       (case when "base" = 'SimpleAggregateFunction' then "saf" else "base" end) as "dtype",
	       (case when "base" = 'SimpleAggregateFunction' then true else false end) as "is_saf",
	       -- NOT NULL only on exact-value numeric/temporal/boolean targets. Character targets are left nullable
	       -- (Exasol stores '' as NULL, and a non-nullable ClickHouse String may be empty); Float too (Exasol
	       -- turns inf/nan into NULL, which a non-nullable ClickHouse Float may legitimately contain).
	       (case when "type" like 'Nullable(%' or "type" like 'LowCardinality(Nullable(%' then 0
	             when (case when "base" = 'SimpleAggregateFunction' then "saf" else "base" end) in ('Int8','Int16','Int32','Int64','UInt8','UInt16','UInt32','UInt64','Int128','Int256','UInt128','UInt256','Decimal','Date','Date32','DateTime','DateTime64','Bool') then 1
	             else 0 end) as "not_null",
	       (case when "default_kind" in ('ALIAS','EPHEMERAL') then true else false end) as "skip_col",
	       cast("nprec" as decimal(18,0)) as "nprec", cast("nscale" as decimal(18,0)) as "nscale",
	       cast("dtprec" as decimal(9,0)) as "dtprec", cast("coct" as decimal(18,0)) as "coct",
	       "database", "table", "name", "position" as "ordinal_position", "type", "default_kind", "default_expr"
	from vv_pre
)
,vv_catchall as (
	select '-- NOTE: column "' || "database" || '"."' || "table" || '"."' || "name" || '" has unmapped ClickHouse type ' || "type" || ' -> migrated via VARCHAR(2000000) catch-all (please review).' as sql_text
	from vv_columns where not (]]..known..[[) and not "skip_col"
	union all
	select '-- NOTE: column "' || "database" || '"."' || "table" || '"."' || "name" || '" has ClickHouse storage kind ' || "default_kind" || ' (not stored in ClickHouse) -> skipped.' as sql_text
	from vv_columns where "skip_col"
	union all
	select '-- NOTE: column "' || "database" || '"."' || "table" || '"."' || "name" || '" is an AggregateFunction (opaque aggregation state) -> migrated as NULL.' as sql_text
	from vv_columns where "dtype" = 'AggregateFunction' and not "skip_col"
)
,vv_skip_engines as (
	select '-- NOTE: table "' || "database" || '"."' || "name" || '" uses ClickHouse engine ' || "engine" || ' (integration/virtual) -> skipped (not migratable as a data table).' as sql_text
	from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select database, name, engine from system.tables t where not ]]..data_eng..[[ and not (]]..view_eng..[[)]]..chflt('t.database','t.name')..[[') se ("database","name","engine")
)
,vv_pk_raw as (select p.* from (import from jdbc at ]]..CONNECTION_NAME..[[ statement ']]..pk_q..[[') p ("database","table","name","column_position") where exists (select 1 from vv_columns c where c."database" = p."database" and c."table" = p."table" and c."name" = p."name" and not c."skip_col"))
,vv_pk as (
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U('"table"')..[[ || '_PK" PRIMARY KEY (' || group_concat('"' || ]]..U('"name"')..[[ || '"' order by "column_position") || ') DISABLE;' as sql_text
	from vv_pk_raw group by "database","table"
)
,vv_create_schemas as (select distinct 'CREATE SCHEMA IF NOT EXISTS "' || "exa_schema" || '";' as sql_text from vv_columns)
,vv_create_tables as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "exa_table" || '" (' || group_concat((]]..coldef..[[) order by "ordinal_position" separator ', ') || ');' as sql_text
	from vv_columns where not "skip_col" group by "exa_schema","exa_table"
)
,vv_imports as (
	select 'IMPORT INTO "' || "exa_schema" || '"."' || "exa_table" || '" FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || 'select ' || group_concat((]]..src..[[) order by "ordinal_position" separator ', ') || ' from "' || "database" || '"."' || "table" || '"' || '''' || ';' as sql_text
	from vv_columns where not "skip_col" group by "exa_schema","exa_table","database","table"
)]]..comments_cte..views_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_catchall
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 9, sql_text from vv_skip_engines
	UNION ALL select 3, cast('-- ### TABLES ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (from the ClickHouse sort key; NON-UNIQUE, created DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk]]..comments_union..[[
	UNION ALL select 50, cast('-- ### IMPORTS ###' as varchar(2000000))
	UNION ALL select 51, sql_text from vv_imports
	UNION ALL select 60, cast('-- ### CONSTRAINT STATE - run AFTER the data load (key created DISABLED for a fast, order-independent load) ###' as varchar(2000000))
	UNION ALL select 61, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U('"table"')..[[ || '_PK" ]]..sw..[[;]]..scomment..[[' from vv_pk_raw group by "database","table"]]..views_union..check_union..[[
) order by ord
]],{})

if not suc then error('"'..res.error_message..'" caught while executing: "'..res.statement_text..'"') end
return(res)
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Prerequisites
--   * The ClickHouse database must be reachable from this Exasol database.
--
-- INSTALL THE CLICKHOUSE JDBC DRIVER IN BUCKETFS  (do this BEFORE the CREATE CONNECTION below):
--   1. Download the ClickHouse JDBC driver from Maven. You MUST take the "-all-dependencies" jar
--      (clickhouse-jdbc-x.x.x-all-dependencies.jar), which bundles everything the driver needs:
--        https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc
--      ClickHouse JDBC driver documentation:
--        https://clickhouse.com/docs/integrations/language-clients/java/jdbc
--   2. Create a plain text file settings.cfg with EXACTLY this content:
--        DRIVERNAME=CLICKHOUSE
--        DRIVERMAIN=com.clickhouse.jdbc.Driver
--        PREFIX=jdbc:clickhouse:
--        NOSECURITY=YES
--        FETCHSIZE=100000
--        INSERTSIZE=-1
--   3. Upload BOTH clickhouse-jdbc-x.x.x-all-dependencies.jar AND settings.cfg into BucketFS
--      (Exasol "add a JDBC driver"):
--        on-premise: https://docs.exasol.com/db/latest/administration/on-premise/manage_drivers/add_jdbc_driver.htm
--        SaaS:       https://docs.exasol.com/db/latest/administration/manage_drivers/add_jdbc_driver.htm
--      On-premise upload example (set WRITE_PW and DATABASE_NODE_IP to your values):
--        curl -k -X PUT -T settings.cfg                              https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/clickhouse/settings.cfg
--        curl -k -X PUT -T clickhouse-jdbc-0.9.8-all-dependencies.jar https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/clickhouse/clickhouse-jdbc-0.9.8-all-dependencies.jar
--
-- Then create a connection to the ClickHouse source database (adjust host, port and credentials), and run the
-- accompanying test query. The default ClickHouse HTTP port is 8123 (native/TCP is 9000; the JDBC driver uses HTTP).

CREATE OR REPLACE CONNECTION CLICKHOUSE_JDBC
    TO 'jdbc:clickhouse://clickhouse_host:8123/'
    USER 'username' IDENTIFIED BY 'password';
SELECT * FROM (IMPORT FROM JDBC AT CLICKHOUSE_JDBC STATEMENT 'SELECT ''Connection works'' AS ok');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.CLICKHOUSE_TO_EXASOL(
    'CLICKHOUSE_JDBC',  -- CONNECTION_NAME: JDBC connection to the ClickHouse source
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted (ClickHouse is case-sensitive - use false if names differ only by case)
    '%',                -- SCHEMA_FILTER: source database(s): 'mydb', 'app_%', '%' (all; system databases always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source ClickHouse database name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; the sort-key PRIMARY KEY kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (key enabled = Exasol re-validates the data; may fail on duplicate sort-key values)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate ClickHouse comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; > 36 digits -> DECIMAL(36,s)), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text) for Int128/256, UInt128/256 and Decimal with precision > 36
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a String/FixedString value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build <table>_MIG_CHK metric tables + a <schema>_MIG_CHK summary (source vs target) for post-load validation
);
