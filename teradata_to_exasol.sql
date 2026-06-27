create schema if not exists database_migration;

/*
    TERADATA_TO_EXASOL - generate the statements to migrate a Teradata (Vantage) database to Exasol.

    Runs on the TARGET Exasol database, reads the SOURCE metadata through a JDBC connection (DBC catalog
    views) and RETURNS the statements (CREATE SCHEMA / CREATE TABLE incl. PRIMARY KEY / FOREIGN KEY /
    DISTRIBUTE BY / PARTITION BY / COMMENTs / IMPORT / a final CONSTRAINT STATE section / optional VIEW
    review). It changes nothing itself - review and run the generated statements in the order returned.

    SOURCE / TARGET: Teradata Vantage 20 and earlier (catalog views and ColumnType codes used here are stable
    back to Teradata 14+) -> Exasol v8 (2025+). Use the Teradata JDBC driver (TeraJDBC). CHARSET=UTF16 is the
    recommended connection setting for Unicode data, but the mapping is correct with UTF8 as well.

    ONLY REAL USER DATA is generated: the built-in system databases (DBC, Sys*, TD_*, TDaaS_*, SYSLIB, ...)
    are always excluded, and only real tables (TableKind 'T' and 'O' = NoPI/columnar) are migrated.

    DATA-TYPE MAPPING (by DBC.ColumnsV ColumnType code; UDTs resolved via ColumnUDTName):
      Exact: BYTEINT/SMALLINT/INTEGER/BIGINT -> DECIMAL(3|5|10|19,0); DECIMAL/NUMERIC(p,s) -> DECIMAL(p,s)
      (p>36 -> DECIMAL_OVERFLOW); NUMBER -> DECIMAL or DOUBLE; FLOAT -> DOUBLE; DATE -> DATE; TIMESTAMP(n) ->
      TIMESTAMP(n) (full precision); CHAR/VARCHAR -> CHAR/VARCHAR UTF8 (length in characters; CHAR>2000 ->
      VARCHAR); CLOB -> VARCHAR(2000000).
      Small documented difference: TIME(n)/TIME WITH TIME ZONE -> VARCHAR (Exasol has no TIME type; offset
      kept as text); TIMESTAMP(n) WITH TIME ZONE -> TIMESTAMP(n) WITH LOCAL TIME ZONE (value converted to the
      UTC instant); BYTE/VARBYTE/BLOB -> faithful base64 text in VARCHAR (BINARY_HANDLING; Exasol has no
      general binary type - the bytes are preserved losslessly as base64 and can be decoded downstream);
      JSON/XML/DATASET/most UDTs -> VARCHAR;
      ST_GEOMETRY (and ST_* / MBR / MBB) -> GEOMETRY (WKT); INTERVAL -> native Exasol INTERVAL or VARCHAR
      (INTERVAL_HANDLING); PERIOD(x) -> two columns x_BEGINNING / x_END.
      Hard limits (the IMPORT fails loudly rather than corrupting data): a value > 2,000,000 characters
      (unless TRUNCATE_LONG_STRINGS=true); DECIMAL/NUMBER values needing > 36 digits under DECIMAL_OVERFLOW='CAP'.

    NLS: IMPORT FROM JDBC transfers TYPED values, so numbers/dates/timestamps are migrated by value and are
    not affected by differing source/target locale settings.

    CONSTRAINTS: PK/FK are always created DISABLED (fast, order-independent load); a final CONSTRAINT STATE
    section sets them per CONSTRAINT_STATE. Exasol uses disabled keys as optimizer/BI metadata, so
    'FORCE_DISABLE' (recommended) is fine and fastest; 'FORCE_ENABLE' makes Exasol re-validate the data.

    DISTRIBUTION / PARTITIONING: the Teradata Primary Index is mapped to an Exasol DISTRIBUTE BY
    (GENERATE_DISTRIBUTION_BY). A single-column Teradata RANGE_N partition is mapped best-effort to an Exasol
    PARTITION BY on that column (GENERATE_PARTITION_BY); multi-level, CASE_N or expression-based PPI has no
    single-column Exasol equivalent and is emitted as a commented manual-review note.

    Not migrated (out of scope): secondary/join/hash indexes, CHECK/UNIQUE constraints (unsupported by
    Exasol), macros/procedures/functions, users/roles/rights.
*/
--/
create or replace script database_migration.TERADATA_TO_EXASOL(
  CONNECTION_NAME               -- name of the JDBC connection inside Exasol -> e.g. TERADATA_JDBC
  ,IDENTIFIER_CASE_INSENSITIVE  -- true (recommended; Teradata names are case-insensitive) => fold ALL identifiers to UPPER so Exasol queries need no quotes; false => keep verbatim/quoted
  ,SCHEMA_FILTER                -- filter for the source databases/schemas (system databases always excluded) -> '%' = all
  ,TABLE_FILTER                 -- filter for the tables/views -> '%' = all
  ,TARGET_SCHEMA                -- target schema on Exasol; '' = use the source database name
  ,CONSTRAINT_STATE             -- 'FORCE_DISABLE' (recommended), 'SET_AS_SOURCE' or 'FORCE_ENABLE'; PK/FK are always created DISABLED, then set after the IMPORTs
  ,GENERATE_COMMENTS            -- true/false: migrate Teradata CommentString as COMMENT ON
  ,GENERATE_VIEWS               -- true/false: emit source views as a commented manual-review section
  ,GENERATE_DISTRIBUTION_BY     -- true/false: map the Teradata Primary Index to an Exasol DISTRIBUTE BY
  ,GENERATE_PARTITION_BY        -- true/false: add a best-effort PARTITION BY from the Teradata partitioning column (single-column RANGE_N) via ALTER TABLE; complex PPI (CASE_N / multi-level / expression) is emitted as a commented manual-review note
  ,BINARY_HANDLING              -- 'BASE64' (recommended; BYTE/VARBYTE/BLOB migrated losslessly as base64 text - Exasol has no binary type) or 'SKIP' (load NULL). Source values >~48000 bytes load as NULL (Teradata VARCHAR limit).
  ,DECIMAL_OVERFLOW             -- 'CAP' (DECIMAL(36,s); IMPORT fails for values > 36 digits) or 'DOUBLE' (loads, ~15 significant digits) for source precision > 36
  ,TRUNCATE_LONG_STRINGS        -- true: values > 2,000,000 chars are cut to 2,000,000 and imported; false: the IMPORT fails on such a value
  ,INTERVAL_HANDLING            -- 'INTERVAL' (native Exasol INTERVAL, computable) or 'VARCHAR' (interval as text)
  ,CHECK_MIGRATION              -- true/false: additionally emit data-validation metrics. Per migrated table a "<table>_MIG_CHK" table is filled with comparable metrics (row/NULL/DISTINCT counts, numeric MIN/MAX/AVG, char length MIN/MAX) computed on BOTH Teradata and Exasol; a "<schema>_MIG_CHK" summary in DATABASE_MIGRATION lists every metric side by side with an OK/DEVIATION status. Run this block AFTER the IMPORTs.
) RETURNS TABLE
AS

-- IDENTIFIER_CASE_INSENSITIVE = true wraps every identifier in upper(...) (stored UPPER CASE), applied
-- consistently to schemas, tables, columns, primary keys, foreign keys, distribution/partition keys, comments.
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
gen_dist     = (GENERATE_DISTRIBUTION_BY == true) or (string.upper(tostring(GENERATE_DISTRIBUTION_BY)) == 'TRUE')
gen_part     = (GENERATE_PARTITION_BY == true) or (string.upper(tostring(GENERATE_PARTITION_BY)) == 'TRUE')
trunc        = (TRUNCATE_LONG_STRINGS == true) or (string.upper(tostring(TRUNCATE_LONG_STRINGS)) == 'TRUE')
-- Teradata binary (BYTE/VARBYTE/BLOB) is migrated faithfully as base64 text. Exasol has no general binary
-- column type, and the JDBC import cannot receive raw binary; FROM_BYTES(col,'base64m') is a lossless byte
-- encoding (preserves leading zero bytes, no line wrapping). Default BASE64; 'SKIP' loads NULL. Values larger
-- than ~48000 source bytes would exceed the Teradata VARCHAR limit once encoded and are loaded as NULL.
binmode = string.upper(tostring(BINARY_HANDLING))
if binmode ~= 'SKIP' then binmode = 'BASE64' end
decof = string.upper(tostring(DECIMAL_OVERFLOW))
if decof ~= 'DOUBLE' then decof = 'CAP' end
ivmode = string.upper(tostring(INTERVAL_HANDLING))
if ivmode ~= 'VARCHAR' then ivmode = 'INTERVAL' end
gen_check = (CHECK_MIGRATION == true) or (string.upper(tostring(CHECK_MIGRATION)) == 'TRUE')

function U(col) return exa_upper_begin..col..exa_upper_end end
-- target schema name expression (TARGET_SCHEMA override or the source schema)
if TARGET_SCHEMA == null then tschema = [["schema_name"]] else tschema = [[']]..TARGET_SCHEMA..[[']] end
sname_e = U(tschema)
-- foreign-key parent schema: when TARGET_SCHEMA is set every table lands there, otherwise the parent keeps its own source schema
if TARGET_SCHEMA == null then ref_sname_e = U('"ref_schema"') else ref_sname_e = sname_e end

-- System databases excluded (Teradata 14-20 / Vantage, current as of 20). Only real user data is generated.
sys_db = [[''All'',''Crashdumps'',''DBC'',''dbcmngr'',''Default'',''External_AP'',''EXTUSER'',''LockLogShredder'',''PUBLIC'',''SAS_SYSFNLIB'',''SQLJ'',''SysAdmin'',''SYSBAR'',''SYSJDBC'',''SYSLIB'',''SYSSPATIAL'',''SystemFe'',''SYSUDTLIB'',''SYSUIF'',''Sys_Calendar'',''TDaaS_BAR'',''TDaaS_DB'',''TDaaS_Maint'',''TDaaS_Monitor'',''TDaaS_Support'',''TDBCMgmt'',''TDMaps'',''TDPUSER'',''TDQCD'',''TDStats'',''tdwm'',''TD_ANALYTICS_DB'',''TD_METRIC_SVC'',''TD_MLDB'',''TD_MODELOPS'',''TD_SERVER_DB'',''TD_SYSAI'',''TD_SYSFNLIB'',''TD_SYSGPL'',''TD_SYSXML'',''TD_VAL'',''val'',''console'',''viewpoint'']]

-------------------------------------------------------------------------------------------------------
-- Remote (Teradata DBC) metadata queries. Inner literals are quote-doubled (embedded in statement '...').
-- char_len: characters (UNICODE ColumnLength is bytes = 2x chars). frac: DecimalFractionalDigits.
-------------------------------------------------------------------------------------------------------
columns_q = [[select trim(c.DatabaseName) as schema_name, trim(c.TableName) as table_name, c.ColumnName as column_name, c.ColumnId as ordinal_position, trim(c.ColumnType) as data_type, trim(c.ColumnUDTName) as udt_name, c.DecimalTotalDigits as num_prec, c.DecimalFractionalDigits as num_scale, c.ColumnLength as col_len, c.CharType as char_type, case when c.CharType = 2 then c.ColumnLength/2 else c.ColumnLength end as char_len, case when c.Nullable = ''Y'' then 1 else 0 end as nullable, trim(c.DefaultValue) as default_value, trim(c.IdColType) as id_col_type, trim(c.CommentString) as col_comment from DBC.ColumnsV c join DBC.TablesV t on c.DatabaseName = t.DatabaseName and c.TableName = t.TableName and t.TableKind in (''T'',''O'') where c.DatabaseName not in (]]..sys_db..[[) and c.DatabaseName like '']]..SCHEMA_FILTER..[['' and c.TableName like '']]..TABLE_FILTER..[['']]

pk_q = [[select trim(i.DatabaseName) as schema_name, trim(i.TableName) as table_name, i.ColumnName as column_name, i.ColumnPosition as column_position from DBC.IndicesV i where i.UniqueFlag = ''Y'' and i.IndexType = ''K'' and i.DatabaseName not in (]]..sys_db..[[) and i.DatabaseName like '']]..SCHEMA_FILTER..[['' and i.TableName like '']]..TABLE_FILTER..[['']]

fk_q = [[select trim(ChildDB) as schema_name, trim(ChildTable) as table_name, trim(IndexName) as fk_name, trim(ChildKeyColumn) as fk_column, trim(ParentDB) as ref_schema, trim(ParentTable) as ref_table, trim(ParentKeyColumn) as ref_column from DBC.All_RI_ChildrenV where ChildDB not in (]]..sys_db..[[) and ChildDB like '']]..SCHEMA_FILTER..[['' and ChildTable like '']]..TABLE_FILTER..[['']]

-------------------------------------------------------------------------------------------------------
-- Exasol-side expressions producing the generated statement text.
-------------------------------------------------------------------------------------------------------
-- DECIMAL / NUMBER overflow.
if decof == 'DOUBLE' then
	dec_t = [[case when "num_prec" is null or "num_prec" > 36 or "num_prec" = -128 then 'DOUBLE' else 'DECIMAL(' || "num_prec" || ',' || (case when "num_scale" < 0 then 0 else "num_scale" end) || ')' end]]
else
	dec_t = [[case when "num_prec" is null or "num_prec" = -128 then 'DOUBLE' when "num_prec" > 36 then 'DECIMAL(36,' || (case when "num_scale" > 36 then 36 when "num_scale" < 0 then 0 else "num_scale" end) || ')' else 'DECIMAL(' || "num_prec" || ',' || (case when "num_scale" < 0 then 0 else "num_scale" end) || ')' end]]
end

-- Binary target type. base64 expands bytes by 4/3; size to the encoded length, capped at the Exasol VARCHAR max.
bin_t = [['VARCHAR(' || (case when "col_len" < 1 or floor(("col_len"+2)/3)*4 > 2000000 then 2000000 else floor(("col_len"+2)/3)*4 end) || ') ASCII']]

-- INTERVAL target type.
if ivmode == 'VARCHAR' then
	iv_t = [['VARCHAR(40) ASCII']]
else
	iv_t = [[case when "data_type" in ('YR','YM','MO') then 'INTERVAL YEAR(4) TO MONTH' else 'INTERVAL DAY(4) TO SECOND(' || (case when "num_scale" between 0 and 9 then "num_scale" else 6 end) || ')' end]]
end

-- Exasol column type for non-period columns (period handled separately producing two columns).
col_t = [[case
	when "data_type" = 'I1' then 'DECIMAL(3,0)'
	when "data_type" = 'I2' then 'DECIMAL(5,0)'
	when "data_type" = 'I' then 'DECIMAL(10,0)'
	when "data_type" = 'I8' then 'DECIMAL(19,0)'
	when "data_type" in ('D','N') then ]]..dec_t..[[
	when "data_type" = 'F' then 'DOUBLE'
	when "data_type" = 'DA' then 'DATE'
	when "data_type" = 'AT' then 'VARCHAR(15) ASCII'
	when "data_type" = 'TZ' then 'VARCHAR(21) ASCII'
	when "data_type" = 'TS' then 'TIMESTAMP(' || (case when "num_scale" between 0 and 9 then "num_scale" else 6 end) || ')'
	when "data_type" = 'SZ' then 'TIMESTAMP(' || (case when "num_scale" between 0 and 9 then "num_scale" else 6 end) || ') WITH LOCAL TIME ZONE'
	when "data_type" = 'CF' then case when "char_len" > 2000 then 'VARCHAR(' || "char_len" || ') UTF8' else 'CHAR(' || "char_len" || ') UTF8' end
	when "data_type" = 'CV' then 'VARCHAR(' || (case when "char_len" < 1 then 2000000 else "char_len" end) || ') UTF8'
	when "data_type" = 'CO' then 'VARCHAR(2000000) UTF8'
	when "data_type" = 'JN' then 'VARCHAR(2000000) UTF8'
	when "data_type" = 'XM' then 'VARCHAR(2000000) UTF8'
	when "data_type" = 'DT' then 'VARCHAR(2000000) UTF8'
	when "data_type" in ('BF','BV','BO') then ]]..bin_t..[[
	when "data_type" in ('YR','YM','MO','DY','DH','DM','DS','HR','HM','HS','MI','MS','SC') then ]]..iv_t..[[
	when "data_type" = 'UT' and "udt_name" in ('ST_GEOMETRY','MBR','MBB') then 'GEOMETRY'
	when "data_type" = 'UT' then 'VARCHAR(2000000) UTF8'
	else 'VARCHAR(2000000) UTF8'
end]]

-- Unsupported = a UDT/type we cannot represent. Here only the internal '++' (TD_ANYTYPE, function sigs) and
-- a null/empty type are treated unsupported; everything else has a mapping above.
unsup = [[("data_type" is null or "data_type" = '++' or "data_type" = '')]]

-- column definition (handles PERIOD -> two columns); "exa_col" is the (cased) column name.
coldef = [[case
	when "data_type" = 'PD' then '"' || "exa_col" || '_BEGINNING" DATE, "' || "exa_col" || '_END" DATE'
	when "data_type" in ('PS','PM') then '"' || "exa_col" || '_BEGINNING" TIMESTAMP(6), "' || "exa_col" || '_END" TIMESTAMP(6)'
	when "data_type" in ('PT','PZ') then '"' || "exa_col" || '_BEGINNING" VARCHAR(21) ASCII, "' || "exa_col" || '_END" VARCHAR(21) ASCII'
	else '"' || "exa_col" || '" ' || (]]..col_t..[[) || (case when "nullable" = 0 then ' NOT NULL' else '' end)
end]]

-- source SELECT expression(s) for the IMPORT (must align positionally with coldef).
if binmode == 'SKIP' then bin_imp = [['cast(null as varchar(10))']] else bin_imp = [['case when bytes(' || "col_q" || ') <= 48000 then from_bytes(' || "col_q" || ', ''''base64m'''') else cast(null as varchar(10)) end']] end
-- CLOB/JSON/XML/DATASET: cast to CLOB on the Teradata side (typed JSON/XML/DATASET arrive as JDBC type
-- OTHER and cannot be received; CLOB transfers fine). Truncate via SUBSTR (Teradata VARCHAR max is 64000,
-- so never cast to VARCHAR(2000000) on the Teradata side).
if trunc then
	clob_imp = [['substr(cast(' || "col_q" || ' as clob),1,2000000)']]
else
	clob_imp = [['cast(' || "col_q" || ' as clob)']]
end
src = [[case
	when "data_type" = 'PD' then 'begin(' || "col_q" || '), end(' || "col_q" || ')'
	when "data_type" in ('PS','PM') then 'cast(begin(' || "col_q" || ') as timestamp(6)), cast(end(' || "col_q" || ') as timestamp(6))'
	when "data_type" in ('PT','PZ') then 'cast(cast(begin(' || "col_q" || ') as time(6)) as varchar(21)), cast(cast(end(' || "col_q" || ') as time(6)) as varchar(21))'
	when "data_type" = 'AT' then 'cast(' || "col_q" || ' as varchar(15))'
	when "data_type" = 'TZ' then 'cast(' || "col_q" || ' as varchar(21))'
	when "data_type" = 'SZ' then 'cast(' || "col_q" || ' at time zone 0 as timestamp(' || (case when "num_scale" between 0 and 9 then "num_scale" else 6 end) || '))'
	when "data_type" in ('BF','BV','BO') then ]]..bin_imp..[[
	when "data_type" in ('CO','JN','XM','DT') then ]]..clob_imp..[[
	when "data_type" = 'UT' and "udt_name" in ('ST_GEOMETRY','MBR','MBB') then "col_q" || '.ST_AsText()'
	when "data_type" = 'UT' then 'cast(' || "col_q" || ' as varchar(32000))'
	when "data_type" in ('YR','YM','MO','DY','DH','DM','DS','HR','HM','HS','MI','MS','SC') then 'cast(' || "col_q" || ' as varchar(40))'
	else "col_q"
end]]

-- DEFAULT mapping (conservative: numeric/string literals + CURRENT_DATE/TIMESTAMP; else skipped).
default_e = [[case
	when "default_value" is null then ''
	when "default_value" REGEXP_LIKE '^[-]{0,1}[0-9]+(\.[0-9]+){0,1}$' then ' DEFAULT ' || "default_value"
	when "default_value" REGEXP_LIKE '^''.*''$' then ' DEFAULT ' || "default_value"
	when upper("default_value") in ('CURRENT_DATE','DATE') then ' DEFAULT CURRENT_DATE'
	when upper("default_value") in ('CURRENT_TIMESTAMP','CURRENT_TIME') then ' DEFAULT CURRENT_TIMESTAMP'
	else ''
end]]

-- constraint-state word + trailing comment (final CONSTRAINT STATE section uses MODIFY CONSTRAINT).
if cstate == 'FORCE_ENABLE' then sw = 'enable'; sc = [[  -- forced ENABLE (Exasol re-validates the data)]]
elseif cstate == 'SET_AS_SOURCE' then sw = 'enable'; sc = [[  -- matches Teradata source (keys active)]]
else sw = 'disable'; sc = [[  -- forced DISABLE (optimizer/BI metadata only; faster)]] end

main_q = [['"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"']]
-- Teradata FKs are often unnamed (All_RI_ChildrenV.IndexName empty) -> generate a deterministic name.
fkname = [[coalesce(nullif("fk_name",''), "table_name" || '_FK_' || "ref_table")]]

-- optional CTEs
dist_cte = ''  dist_union = ''
if gen_dist then
	dist_cte = [[
,td_dist as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(DatabaseName) as schema_name, trim(TableName) as table_name, ColumnName as column_name, ColumnPosition as column_position from DBC.IndicesV where IndexType in (''P'',''Q'') and DatabaseName not in (]]..sys_db..[[) and DatabaseName like '']]..SCHEMA_FILTER..[['' and TableName like '']]..TABLE_FILTER..[[''') )
,vv_dist as (select 'ALTER TABLE ' || ]]..main_q..[[ || ' DISTRIBUTE BY ' || group_concat('"' || ]]..U('"column_name"')..[[ || '"' order by "column_position") || ';' as sql_text from td_dist group by "schema_name","table_name")]]
	dist_union = "\n".. [[UNION ALL select 35, cast('-- ### DISTRIBUTE BY (from Teradata Primary Index) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 36, sql_text from vv_dist]]
end

comments_cte = ''  comments_union = ''
if gen_comments then
	comments_cte = [[
,vv_tcomments as (select distinct * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(DatabaseName) as schema_name, trim(TableName) as table_name, trim(CommentString) as tcomment from DBC.TablesV where TableKind in (''T'',''O'') and CommentString is not null and DatabaseName not in (]]..sys_db..[[) and DatabaseName like '']]..SCHEMA_FILTER..[['' and TableName like '']]..TABLE_FILTER..[[''') )
,vv_comment_tab as (select 'COMMENT ON TABLE ' || ]]..main_q..[[ || ' IS ' || '''' || replace("tcomment", '''', '''''') || '''' || ';' as sql_text from vv_tcomments)
,vv_comment_col as (select 'COMMENT ON COLUMN ' || ]]..main_q..[[ || '."' || ]]..U('"column_name"')..[[ || '"' || ' IS ' || '''' || replace("col_comment", '''', '''''') || '''' || ';' as sql_text from vv_columns where "col_comment" is not null and not ]]..unsup..[[ and "data_type" not in ('PD','PS','PM','PT','PZ'))]]
	comments_union = "\n".. [[UNION ALL select 41, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 42, sql_text from vv_comment_tab
UNION ALL select 43, sql_text from vv_comment_col]]
end

views_cte = ''  views_union = ''
if gen_views then
	views_cte = [[
,vv_views_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(DatabaseName) as schema_name, trim(TableName) as view_name, RequestText as view_text from DBC.TablesV where TableKind = ''V'' and DatabaseName not in (]]..sys_db..[[) and DatabaseName like '']]..SCHEMA_FILTER..[['' and TableName like '']]..TABLE_FILTER..[[''') )
,vv_views as (select '-- ' || "schema_name" || '.' || "view_name" || '  (Teradata view - review and adapt to Exasol SQL manually):' || chr(10) || '-- ' || replace("view_text", chr(10), chr(10) || '-- ') as sql_text from vv_views_raw)]]
	views_union = "\n".. [[UNION ALL select 90, cast('-- ### VIEWS (Teradata SQL - commented out, manual review required) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 91, sql_text from vv_views]]
end

-- GENERATE_PARTITION_BY: best-effort Exasol PARTITION BY from the Teradata partitioning column. Teradata
-- partitioning (PPI) is expression-based (RANGE_N / CASE_N); for a single-column RANGE_N the partition column is
-- extracted and applied via ALTER TABLE ... PARTITION BY (Exasol partitions by column value - a recommended
-- pattern for e.g. a date column). Multi-level, CASE_N or expression-based PPI has no single-column Exasol
-- equivalent and is emitted as a commented manual-review note instead.
part_cte = ''  part_union = ''
if gen_part then
	part_cte = [[
,vv_part_raw as (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select trim(DatabaseName) as schema_name, trim(TableName) as table_name, trim(ConstraintText) as part_text from DBC.PartitioningConstraintsV where DatabaseName not in (]]..sys_db..[[) and DatabaseName like '']]..SCHEMA_FILTER..[['' and TableName like '']]..TABLE_FILTER..[[''') )
,vv_partcol_raw as (
	select "schema_name","table_name","part_text",
	       case when instr("part_text",'CASE_N') = 0 and instr("part_text",'RANGE_N',1,2) = 0 and instr("part_text",'RANGE_N(') > 0 and instr("part_text",' BETWEEN') > instr("part_text",'RANGE_N(') + 8
	            then trim(substr("part_text", instr("part_text",'RANGE_N(') + 8, instr("part_text",' BETWEEN') - instr("part_text",'RANGE_N(') - 8))
	            else null end as "raw_col"
	from vv_part_raw)
,vv_part as (
	select 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" PARTITION BY "' || ]]..U('"raw_col"')..[[ || '";' as sql_text
	from vv_partcol_raw where "raw_col" is not null and "raw_col" REGEXP_LIKE '^[A-Za-z_][A-Za-z0-9_]*$'
	union all
	select '-- "' || ]]..U('"schema_name"')..[[ || '"."' || ]]..U('"table_name"')..[[ || '" Teradata partitioning not auto-mapped (review and add PARTITION BY manually if appropriate): ' || "part_text" as sql_text
	from vv_partcol_raw where "raw_col" is null or not "raw_col" REGEXP_LIKE '^[A-Za-z_][A-Za-z0-9_]*$')]]
	part_union = "\n".. [[UNION ALL select 37, cast('-- ### PARTITION BY (best-effort from the Teradata partitioning column; complex PPI listed as a review note) ###' as varchar(2000000)) SQL_TEXT
UNION ALL select 38, sql_text from vv_part]]
end

-- CHECK_MIGRATION: per migrated table a wide single-scan metrics row is computed on BOTH systems (typed, so
-- the target stores both values with identical Exasol types); a per-schema summary unpivots and joins them,
-- flagging each metric OK/DEVIATION. Metrics are chosen to be cross-database comparable so faithfully migrated
-- data produces no false deviations: row/NULL/DISTINCT counts (where reliable), numeric MIN/MAX/SUM, date and
-- timestamp MIN/MAX, char length MIN/MAX. Binary/LOB/geometry/UDT columns get NULL counts only.
check_cte = ''  check_union = ''
if gen_check then
	check_cte = [[
,vv_chk_cols as (select c.* from vv_columns c where c."data_type" is not null and c."data_type" not in ('++'))
,vv_chk_base as (
	select x.*, min("ordinal_position") over (partition by "exa_schema","exa_table") as "min_ord",
	       sysrow."db_system", mid."metric_id",
	       case when sysrow."db_system" = 'Exasol' then '"' || x."exa_col" || '"' else x."col_q" end as "ref"
	from vv_chk_cols x
	cross join (select 'Exasol' as "db_system" union all select 'Teradata' as "db_system") sysrow
	cross join (select level-1 as "metric_id" from dual connect by level <= 8) mid
)
,vv_chk_expr as (
	select "exa_schema","exa_table","schema_name","table_name","exa_col","col_q","ordinal_position","db_system","metric_id", "exa_table" || '_MIG_CHK' as "wide_name",
	       (case
	          when "metric_id" = 0 and "ordinal_position" = "min_ord" then 'cast(count(*) as decimal(36,0))'
	          when "metric_id" = 1 and "data_type" in ('PD','PS','PM','PT','PZ') and "nullable" = 1 then 'cast(count(case when ' || (case when "db_system" = 'Exasol' then '"' || "exa_col" || '_BEGINNING"' else "col_q" end) || ' is null then 1 end) as decimal(36,0))'
	          when "metric_id" = 1 and "nullable" = 1 and "data_type" not in ('PD','PS','PM','PT','PZ') then 'cast(count(case when ' || "ref" || ' is null then 1 end) as decimal(36,0))'
	          when "metric_id" = 2 and "data_type" in ('I1','I2','I','I8') then 'cast(min(' || "ref" || ') as decimal(20,0))'
	          when "metric_id" = 2 and "data_type" in ('N','D') and "num_prec" between 1 and 36 then 'cast(min(' || "ref" || ') as decimal(36,' || (case when "num_scale" is null or "num_scale" < 0 then 0 when "num_scale" > 36 then 36 else "num_scale" end) || '))'
	          when "metric_id" = 2 and "data_type" in ('DA','TS') then 'min(' || "ref" || ')'
	          when "metric_id" = 3 and "data_type" in ('I1','I2','I','I8') then 'cast(max(' || "ref" || ') as decimal(20,0))'
	          when "metric_id" = 3 and "data_type" in ('N','D') and "num_prec" between 1 and 36 then 'cast(max(' || "ref" || ') as decimal(36,' || (case when "num_scale" is null or "num_scale" < 0 then 0 when "num_scale" > 36 then 36 else "num_scale" end) || '))'
	          when "metric_id" = 3 and "data_type" in ('DA','TS') then 'max(' || "ref" || ')'
	          when "metric_id" = 4 and "data_type" in ('I1','I2','I','I8','F','N','D','DA','TS','SZ','AT','TZ','YR','YM','MO','DY','DH','DM','DS','HR','HM','HS','MI','MS','SC') then 'cast(count(distinct ' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 4 and "data_type" in ('CF','CV') then 'cast(count(distinct ' || "ref" || (case when "db_system" = 'Teradata' then ' (casespecific)' else '' end) || ') as decimal(36,0))'
	          when "metric_id" = 4 and "data_type" in ('PD','PS','PM','PT','PZ') then (case when "db_system" = 'Exasol' then 'cast(count(distinct (' || '"' || "exa_col" || '_BEGINNING", "' || "exa_col" || '_END")) as decimal(36,0))' else 'cast(count(distinct ' || "col_q" || ') as decimal(36,0))' end)
	          when "metric_id" = 5 and "data_type" in ('I1','I2','I','I8') then 'cast(sum(' || "ref" || ') as decimal(36,0))'
	          when "metric_id" = 5 and "data_type" in ('N','D') and "num_prec" between 1 and 36 then 'cast(sum(' || "ref" || ') as decimal(36,' || (case when "num_scale" is null or "num_scale" < 0 then 0 when "num_scale" > 36 then 36 else "num_scale" end) || '))'
	          when "metric_id" = 6 and "data_type" in ('CF','CV') and not ("data_type" = 'CF' and "char_len" > 2000) then 'cast(min(' || (case when "db_system" = 'Exasol' then 'length(' else 'character_length(' end) || "ref" || ')) as decimal(36,0))'
	          when "metric_id" = 7 and "data_type" in ('CF','CV') and not ("data_type" = 'CF' and "char_len" > 2000) then 'cast(max(' || (case when "db_system" = 'Exasol' then 'length(' else 'character_length(' end) || "ref" || ')) as decimal(36,0))'
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
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "wide_name" || '" AS ' || max(case when "db_system" = 'Exasol' then "sys_select" end) || ' UNION ALL select * from (IMPORT FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || replace(max(case when "db_system" = 'Teradata' then "sys_select" end), '''', '''''') || '''' || ') ;' as sql_text
	from vv_chk_sys group by "exa_schema","wide_name"
)
,vv_chk_unpiv as (
	select "exa_schema","exa_table","ordinal_position","metric_id","db_system",
	       'select ' || '''' || "exa_table" || '''' || ' as "TABLE_NAME", ' || '''' || "metric_name" || '''' || ' as "METRIC", to_char("' || "metric_name" || '") as "VAL" from "' || "exa_schema" || '"."' || "wide_name" || '" where "DB_SYSTEM" = ' || '''' || "db_system" || '''' as "frag"
	from vv_chk_named
)
,vv_chk_summary as (
	select 'CREATE OR REPLACE TABLE "DATABASE_MIGRATION"."' || "exa_schema" || '_MIG_CHK" AS select e."TABLE_NAME", e."METRIC", e."VAL" as "EXASOL_METRIC", t."VAL" as "TERADATA_METRIC", case when coalesce(e."VAL", ''~NULL~'') = coalesce(t."VAL", ''~NULL~'') then ''OK'' else ''DEVIATION'' end as "STATUS" from (' || listagg(case when "db_system" = 'Exasol' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') e join (' || listagg(case when "db_system" = 'Teradata' then "frag" end, ' union all ') within group (order by "exa_table","ordinal_position","metric_id") || ') t on e."TABLE_NAME" = t."TABLE_NAME" and e."METRIC" = t."METRIC" order by "STATUS" desc, e."TABLE_NAME", e."METRIC";' as sql_text
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
	select '-- !!! UNSUPPORTED TYPE - column NOT migrated: "' || "schema_name" || '"."' || "table_name" || '"."' || "column_name" || '" (Teradata type code: ' || coalesce("data_type",'null') || coalesce(' / ' || "udt_name",'') || ') - migrate this column manually !!!' as sql_text
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
	select 'ALTER TABLE ' || '"' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '"' || ' ADD CONSTRAINT "' || ]]..U(fkname)..[[ || '" FOREIGN KEY (' || group_concat('"' || ]]..U('"fk_column"')..[[ || '"') || ') REFERENCES "' || ]]..ref_sname_e..[[ || '"."' || ]]..U('"ref_table"')..[[ || '" (' || group_concat('"' || ]]..U('"ref_column"')..[[ || '"') || ') DISABLE;' as sql_text
	from vv_fk_raw group by "schema_name","table_name","fk_name","ref_schema","ref_table"
)
,vv_create_schemas as (select distinct 'CREATE SCHEMA IF NOT EXISTS "' || "exa_schema" || '";' as sql_text from vv_columns)
,vv_create_tables as (
	select 'CREATE OR REPLACE TABLE "' || "exa_schema" || '"."' || "exa_table" || '" (' || group_concat(case when ]]..unsup..[[ then null else (]]..coldef..[[) || ]]..default_e..[[ end order by "ordinal_position" separator ', ') || ');' as sql_text
	from vv_columns group by "exa_schema","exa_table"
)
,vv_imports as (
	select 'IMPORT INTO "' || "exa_schema" || '"."' || "exa_table" || '" FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT ' || '''' || 'select ' || group_concat(case when ]]..unsup..[[ then null else (]]..src..[[) end order by "ordinal_position" separator ', ') || ' from "' || "schema_name" || '"."' || "table_name" || '"' || '''' || ';' as sql_text
	from vv_columns group by "exa_schema","exa_table","schema_name","table_name"
)]]..dist_cte..comments_cte..views_cte..part_cte..check_cte..[[
select sql_text from (
	select 0 ord, sql_text SQL_TEXT from vv_unsupported
	UNION ALL select 1, cast('-- ### SCHEMAS ###' as varchar(2000000))
	UNION ALL select 2, sql_text from vv_create_schemas
	UNION ALL select 3, cast('-- ### TABLES (incl. PRIMARY KEY, created DISABLED) ###' as varchar(2000000))
	UNION ALL select 4, sql_text from vv_create_tables where sql_text not like '%();%'
	UNION ALL select 5, cast('-- ### PRIMARY KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 6, sql_text from vv_pk
	UNION ALL select 7, cast('-- ### FOREIGN KEYS (DISABLED) ###' as varchar(2000000))
	UNION ALL select 8, sql_text from vv_fk]]..dist_union..comments_union..[[
	UNION ALL select 50, cast('-- ### IMPORTS ###' as varchar(2000000))
	UNION ALL select 51, sql_text from vv_imports
	UNION ALL select 60, cast('-- ### CONSTRAINT STATE - run AFTER the data load (keys created DISABLED for a fast, order-independent load) ###' as varchar(2000000))
	UNION ALL select 61, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U('"table_name"')..[[ || '_PK" ]]..sw..[[;]]..sc..[[' from vv_pk_raw group by "schema_name","table_name"
	UNION ALL select 62, 'ALTER TABLE "' || ]]..sname_e..[[ || '"."' || ]]..U('"table_name"')..[[ || '" MODIFY CONSTRAINT "' || ]]..U(fkname)..[[ || '" ]]..sw..[[;]]..sc..[[' from vv_fk_raw group by "schema_name","table_name","fk_name","ref_table"]]..views_union..part_union..check_union..[[
) order by ord
]],{})

if not suc then error('"'..res.error_message..'" caught while executing: "'..res.statement_text..'"') end
return(res)
/

-- ===================================================================================================
-- CONNECTION SETUP
-- ===================================================================================================
-- Prerequisites
--   * The Teradata database must be reachable from this Exasol database.
--   * The credentials used in the connection must be valid.
--   * Use the latest Teradata JDBC driver (terajdbc).
--
-- JDBC driver (install once in BucketFS - the driver and its settings.cfg)
--   * terajdbc 20.00.00.58 or higher
--       https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc
--   * Driver setup guide:
--       https://docs.exasol.com/db/latest/loading_data/connect_sources/teradata.htm
--   * Teradata to Exasol Migration Guide:
--       https://docs.exasol.com/db/latest/migration_guides/teradata/teradata_exasol.htm
--
-- Create a connection to the Teradata database (adjust host, database name and credentials),
-- then run the accompanying test query.
-- CHARSET=UTF16 is the recommended setting for Unicode data.

CREATE OR REPLACE CONNECTION TERADATA_JDBC
    TO 'jdbc:teradata://teradata_host/CHARSET=UTF16,DBS_PORT=1025,DATABASE=my_teradata_db'
    USER 'dbc' IDENTIFIED BY 'dbc';
SELECT * FROM (IMPORT FROM JDBC AT TERADATA_JDBC STATEMENT 'SELECT ''Connection works''');

-- ===================================================================================================
-- GENERATE THE MIGRATION STATEMENTS (recommended defaults shown)
-- ===================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.TERADATA_TO_EXASOL(
    'TERADATA_JDBC',    -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes (Teradata identifiers are case-insensitive, so nothing is lost); false => keep verbatim/quoted (preserves lower/MixedCase, but every query must quote them)
    '%',                -- SCHEMA_FILTER: source database(s)/schema(s): 'CORE', 'MART_%', '%' (all; system databases are always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'H_EMPLOYEE', 'H_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source database name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Teradata comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_DISTRIBUTION_BY: true => map the Teradata Primary Index to an Exasol DISTRIBUTE BY; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from the Teradata partitioning column (single-column RANGE_N) inside the CREATE TABLE; complex PPI (CASE_N / multi-level / expression) is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; BYTE/VARBYTE/BLOB migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; DECIMAL(36,s), import fails for values needing > 36 digits) or 'DOUBLE' (loads with ~15 significant digits)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'INTERVAL',         -- INTERVAL_HANDLING: 'INTERVAL' (recommended; native Exasol INTERVAL, computable) or 'VARCHAR' (interval as text)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build per-table "<table>_MIG_CHK" metric tables and a "<schema>_MIG_CHK" summary that compares source vs. target (run after the IMPORTs)
);
