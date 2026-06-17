create schema if not exists database_migration;

/*
    EXASOL_TO_EXASOL - generate the statements to migrate an Exasol database to another Exasol database.

    The script runs on the TARGET database, reads the SOURCE metadata through a database connection
    (EXA or JDBC) and RETURNS the statements (CREATE SCHEMA / CREATE TABLE / constraints / DISTRIBUTE BY /
    PARTITION BY / COMMENTs / IMPORT / CREATE VIEW) needed to recreate and load the source. It changes
    nothing itself - you review and run the generated statements.

    Data-type mapping is 1:1: every column keeps its source COLUMN_TYPE verbatim, so ALL data types are
    covered automatically (incl. the character set, e.g. VARCHAR(n) ASCII / UTF8) and new types need no
    maintenance. Because IMPORT transfers TYPED values (not formatted strings), differing NLS settings
    between source and target do NOT affect the data (dates, timestamps incl. nanoseconds, decimals and
    their separators are preserved over both EXA and JDBC connections).

    Compatibility:
      - 7.1 -> 8 and 7.1 -> 7.1 work out of the box (the source renders its own type strings).
      - For a DOWNGRADE (e.g. 8 -> 7.1) set TARGET_VERSION = '7': Exasol 8 renders TIMESTAMP as
        TIMESTAMP(p), which 7.x cannot parse; TARGET_VERSION='7' strips the precision (TIMESTAMP(p) ->
        TIMESTAMP, CURRENT_TIMESTAMP(p) -> CURRENT_TIMESTAMP).

    What is migrated: schemas, tables (columns/types incl. charset, NOT NULL, IDENTITY, DEFAULT),
    primary keys, foreign keys, distribution keys, partition keys, table & column comments, data, views
    (created WITH FORCE so view-to-view ordering does not matter). System schemas (SYS, EXA_STATISTICS)
    and VIRTUAL schema objects are excluded.

    Loading tip: data loads MUCH faster while primary/foreign keys are disabled. Run with PK_SETTING =
    'DISABLE' (recommended): the keys are created in the DISABLEd state and the script appends a final
    "ENABLE PRIMARY & FOREIGN KEYS" section. Run that section AFTER the IMPORTs to (re)validate and
    activate every key (primary keys are enabled before foreign keys). With PK_SETTING = 'ENABLE' the keys
    are active immediately and no ENABLE section is generated.

    Not migrated (out of scope): functions, scripts/UDFs/adapters, users/roles/privileges, connections.

    Connection: read over the native EXA or JDBC interface - both are built into Exasol (no driver to
    install). Prefer EXA: IMPORT FROM EXA is always parallelized, so loading directly from another Exasol
    database is significantly faster.

    Privileges: the source metadata is read from the EXA_ALL_* views THROUGH the connection's user, so the
    script only sees - and only generates statements for - the objects that user may access on the source;
    the generated statements run on the target only where you have the matching privileges. To migrate
    everything, use a user with DBA privileges on BOTH the source and the target.

    Known limitation: a view body is copied verbatim; if you store identifiers case-insensitively
    (IDENTIFIER_CASE_INSENSITIVE = true) but the source view text uses quoted lower/mixed-case names, the
    view text is NOT re-cased and may need a manual adjustment. View comments are emitted as part of the
    CREATE VIEW (COMMENT ON VIEW is not supported by Exasol).
*/
--/
create or replace script database_migration.EXASOL_TO_EXASOL(
  CONNECTION_NAME              -- name of the database connection inside Exasol -> e.g. my_exa
  ,CONNECTION_SETTING          -- set to EXA (native, parallel, faster) or JDBC; both are built into Exasol (no driver to install)
  ,IDENTIFIER_CASE_INSENSITIVE -- true: store identifiers case-insensitively (folded to UPPER); false: case-sensitive (verbatim, quoted)
  ,SCHEMA_FILTER               -- filter for the schemas to generate and load (SYS, EXA_STATISTICS and virtual schemas are always excluded) -> '%' for all
  ,TABLE_FILTER                -- filter for the tables to generate and load -> '%' for all
  ,GENERATE_VIEWS              -- true/false: include views
  ,VIEW_FILTER                 -- filter for the views to generate -> '%' for all
  ,PK_SETTING                  -- ENABLE or DISABLE: state of the generated primary/foreign key constraints. 'DISABLE' = much faster load; an "ENABLE PRIMARY & FOREIGN KEYS" section is appended to run after the IMPORTs
  ,TARGET_VERSION              -- target Exasol major version: '8' (default, no change) or '7' (downgrade TIMESTAMP(p) -> TIMESTAMP)
) RETURNS TABLE
AS

-- IDENTIFIER_CASE_INSENSITIVE = true wraps every identifier in upper(...) so it is stored UPPER CASE.
-- The wrapper is applied CONSISTENTLY to schemas, tables, columns, primary keys, foreign keys,
-- distribution keys, partition keys and comments (previously only schemas/tables/columns were wrapped,
-- which broke the FK/PARTITION/DISTRIBUTE statements for mixed-case sources).
exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

-- Robust view flag: accept boolean true or the string 'TRUE' (any case). The view CTE and the view
-- output rows are only added when this is true (instead of injecting a raw SQL boolean into the query).
gen_views = (GENERATE_VIEWS == true) or (string.upper(tostring(GENERATE_VIEWS)) == 'TRUE')

-- TARGET_VERSION downgrade: for an Exasol 7.x target, strip the fractional-seconds precision that only
-- Exasol 8 understands. ts_open/ts_close wrap the whole column-type expression in a REGEXP_REPLACE that
-- turns TIMESTAMP(p) -> TIMESTAMP (this also fixes a CURRENT_TIMESTAMP(p) default).
ts_open=''
ts_close=''
if string.sub(tostring(TARGET_VERSION),1,1) == '7' then
	ts_open='REGEXP_REPLACE('
	ts_close=[[, ''TIMESTAMP\([0-9]+\)'', ''TIMESTAMP'')]]
end

-- Build the optional view CTE / output rows only when views are requested.
view_cte=''
view_union=''
if gen_views then
	view_cte=[[
,vv_create_views as (
  -- VIEW_TEXT already contains any view COMMENT (Exasol stores the comment inside the view definition and
  -- COMMENT ON VIEW is not supported), so the comment migrates automatically. We only normalize the leading
  -- CREATE [OR REPLACE] [FORCE] VIEW to "CREATE OR REPLACE FORCE VIEW" so view-to-view dependencies never
  -- block the migration (FORCE lets a view be created before the objects it references exist).
  -- The regex avoids the question-mark character (some SQL clients, e.g. DbVisualizer, treat a literal
  -- question mark as a bind marker at install time): case-insensitivity via character classes, "optional"
  -- via {0,1}.
  select REGEXP_REPLACE(view_text, '^\s*[Cc][Rr][Ee][Aa][Tt][Ee]\s+([Oo][Rr]\s+[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\s+){0,1}([Ff][Oo][Rr][Cc][Ee]\s+){0,1}[Vv][Ii][Ee][Ww]', 'CREATE OR REPLACE FORCE VIEW') || ';' as sql_text
  from (import from ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ statement
    'select view_text from EXA_ALL_VIEWS
       where view_schema like '']]..SCHEMA_FILTER..[[''
       and view_name like '']]..VIEW_FILTER..[[''
       order by view_schema, view_name') as exasql
)]]
	view_union="\n".. [[
UNION ALL
select 19, cast('-- ### VIEWS (created WITH FORCE - order-independent) ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 20, g.* from vv_create_views g]]
end

-- When the constraints are generated DISABLEd (PK_SETTING = 'DISABLE'), also emit the statements that
-- ENABLE every primary and foreign key afterwards. Loading data is much faster while keys are disabled;
-- run this section AFTER the IMPORTs to (re)validate and activate the keys. Primary keys are enabled
-- before foreign keys (sections 17 then 18). Not generated when the keys are already created ENABLEd.
enable_cte=''
enable_union=''
if string.upper(tostring(PK_SETTING)) == 'DISABLE' then
	enable_cte=[[
,vv_enable_keys as (
  select alter_enable, key_ord from
   (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
   'select CONCAT(''ALTER TABLE "'',]]..exa_upper_begin..[[CONSTRAINT_SCHEMA]]..exa_upper_end..[[,''"."'',]]..exa_upper_begin..[[CONSTRAINT_TABLE]]..exa_upper_end..[[,''" MODIFY CONSTRAINT "'',]]..exa_upper_begin..[[CONSTRAINT_NAME]]..exa_upper_end..[[,''" ENABLE;'') as alter_enable,
           CASE WHEN CONSTRAINT_TYPE = ''PRIMARY KEY'' THEN 1 ELSE 2 END as key_ord
    from "SYS"."EXA_ALL_CONSTRAINT_COLUMNS"
    WHERE CONSTRAINT_TYPE IN (''PRIMARY KEY'',''FOREIGN KEY'')
    AND CONSTRAINT_SCHEMA like '']]..SCHEMA_FILTER..[[''
    AND CONSTRAINT_TABLE like '']]..TABLE_FILTER..[[''
    GROUP BY CONSTRAINT_SCHEMA, CONSTRAINT_TABLE, CONSTRAINT_NAME, CONSTRAINT_TYPE') as enable_keys
)]]
	enable_union="\n".. [[
UNION ALL
select 16, cast('-- ### ENABLE PRIMARY & FOREIGN KEYS - run AFTER the data load (loading is much faster with keys disabled) ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 17, ek.alter_enable from vv_enable_keys ek where ek.key_ord = 1
UNION ALL
select 18, ek2.alter_enable from vv_enable_keys ek2 where ek2.key_ord = 2]]
end

suc, res = pquery([[
with vv_exa_columns as (
  select ]]..exa_upper_begin..[[table_schema]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[[table_name]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", exasql.* from
		(import from ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ statement
			'with constr_cols as (
                                select *
                                from "SYS"."EXA_ALL_CONSTRAINT_COLUMNS"
                                where CONSTRAINT_TYPE = ''PRIMARY KEY''
                                AND constraint_schema like '']]..SCHEMA_FILTER..[[''
                                AND constraint_table like '']]..TABLE_FILTER..[[''
                                )
                                select table_schema, table_name, c.column_name, COLUMN_ORDINAL_POSITION ordinal_position,
                                       ]]..ts_open..[[COLUMN_TYPE
                                         || CASE WHEN COLUMN_IDENTITY IS NOT NULL THEN '' IDENTITY '' || COLUMN_IDENTITY END
                                         || CASE WHEN COLUMN_DEFAULT IS NOT NULL THEN '' DEFAULT '' || COLUMN_DEFAULT END
                                         || CASE WHEN COLUMN_IS_NULLABLE = FALSE THEN '' NOT NULL'' END]]..ts_close..[[ data_type,
                                       column_type, COLUMN_MAXSIZE character_maximum_length, COLUMN_NUM_PREC numeric_precision, COLUMN_NUM_SCALE numeric_scale,
                                       cc.constraint_name, cc.constraint_type, cc.ordinal_position pk_ordinal,
                                       t.table_comment, c.column_comment
                                        from EXA_ALL_COLUMNS c
                                        join EXA_ALL_TABLES t on t.table_schema = c.column_schema and t.table_name = c.column_table
                                        left join constr_cols cc on cc.constraint_schema = c.column_schema and cc.constraint_table = c.column_table and cc.column_name = c.column_name
                                        where table_schema not in (''SYS'',''EXA_STATISTICS'')
                                        AND t.table_is_virtual = FALSE
                                        AND c.column_is_virtual = FALSE
                                        AND table_schema like '']]..SCHEMA_FILTER..[[''
                                        AND table_name like '']]..TABLE_FILTER..[[''
                                        ORDER BY c."COLUMN_ORDINAL_POSITION"
                        ') as exasql
)
,vv_create_schemas as(
  SELECT 'CREATE SCHEMA IF NOT EXISTS "' || "exa_table_schema" || '";' as sql_text from vv_exa_columns group by "exa_table_schema" order by "exa_table_schema"
)
,vv_create_tables as (
  select 'CREATE OR REPLACE TABLE "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' || data_type ||' ' order by ordinal_position) || vv_pk_constraints.PK_CON || ');' as sql_text
	from vv_exa_columns
	left join (select "exa_table_schema" as pk_schema, "exa_table_name" as pk_table,
                        (', CONSTRAINT "' || ]]..exa_upper_begin..[[constraint_name]]..exa_upper_end..[[ || '" PRIMARY KEY (' || GROUP_CONCAT('"' || "exa_column_name" || '"' ORDER BY pk_ordinal)) || ') ]]..PK_SETTING..[[' as PK_CON
                        from vv_exa_columns
                        where constraint_type = 'PRIMARY KEY'
                       GROUP BY "exa_table_schema", "exa_table_name", constraint_name) as vv_pk_constraints on vv_exa_columns."exa_table_schema" = vv_pk_constraints.pk_schema and vv_exa_columns."exa_table_name" = vv_pk_constraints.pk_table
	group by "exa_table_schema", "exa_table_name", "PK_CON"
	order by "exa_table_schema","exa_table_name", "PK_CON"
)
,vv_create_foreignkey AS (
        select * from
         (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
         'select CONCAT(''ALTER TABLE "'',]]..exa_upper_begin..[[CONSTRAINT_SCHEMA]]..exa_upper_end..[[,''"."'',]]..exa_upper_begin..[[CONSTRAINT_TABLE]]..exa_upper_end..[[,''" ADD CONSTRAINT "'',]]..exa_upper_begin..[[CONSTRAINT_NAME]]..exa_upper_end..[[,''" FOREIGN KEY ('',GROUP_CONCAT(CONCAT(''"'',]]..exa_upper_begin..[[COLUMN_NAME]]..exa_upper_end..[[,''"'') ORDER BY ORDINAL_POSITION),'') REFERENCES "'',]]..exa_upper_begin..[[REFERENCED_SCHEMA]]..exa_upper_end..[[,''"."'',]]..exa_upper_begin..[[REFERENCED_TABLE]]..exa_upper_end..[[,''" ]]..PK_SETTING..[[;'') AS ALTER_TABLE
         from "SYS"."EXA_ALL_CONSTRAINT_COLUMNS"
         WHERE "EXA_ALL_CONSTRAINT_COLUMNS"."CONSTRAINT_TYPE" = ''FOREIGN KEY''
         AND "CONSTRAINT_SCHEMA" like '']]..SCHEMA_FILTER..[[''
         AND "CONSTRAINT_TABLE" like '']]..TABLE_FILTER..[[''
         GROUP BY "CONSTRAINT_SCHEMA", "CONSTRAINT_TABLE","CONSTRAINT_NAME","REFERENCED_SCHEMA","REFERENCED_TABLE"') AS foreign_keys
)
,vv_create_distribution_key as(
select * from
         (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
         'select CONCAT('' ALTER TABLE "'',]]..exa_upper_begin..[[COLUMN_SCHEMA]]..exa_upper_end..[[,''"."'',]]..exa_upper_begin..[[COLUMN_TABLE]]..exa_upper_end..[[,''" DISTRIBUTE BY '',group_concat(concat(''"'',]]..exa_upper_begin..[[COLUMN_NAME]]..exa_upper_end..[[,''"'') ORDER BY COLUMN_ORDINAL_POSITION),'';'')
         from "SYS"."EXA_ALL_COLUMNS"
         WHERE "EXA_ALL_COLUMNS"."COLUMN_IS_DISTRIBUTION_KEY" = TRUE
         AND COLUMN_IS_VIRTUAL = FALSE
         AND COLUMN_SCHEMA like '']]..SCHEMA_FILTER..[[''
         AND COLUMN_TABLE like '']]..TABLE_FILTER..[[''
         GROUP BY "EXA_ALL_COLUMNS"."COLUMN_SCHEMA","EXA_ALL_COLUMNS"."COLUMN_TABLE"') AS distribution_keys
)
,vv_create_partion_key as(
select alter_partion from
         (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
         'SELECT CONCAT('' ALTER TABLE "'',]]..exa_upper_begin..[[COLUMN_SCHEMA]]..exa_upper_end..[[,''"."'',]]..exa_upper_begin..[[COLUMN_TABLE]]..exa_upper_end..[[,''" PARTITION BY '',GROUP_CONCAT(CONCAT(''"'',]]..exa_upper_begin..[[COLUMN_NAME]]..exa_upper_end..[[,''"'') ORDER BY COLUMN_PARTITION_KEY_ORDINAL_POSITION),'';'') as alter_partion
                FROM "SYS"."EXA_ALL_COLUMNS"
                WHERE "EXA_ALL_COLUMNS"."COLUMN_PARTITION_KEY_ORDINAL_POSITION" > 0
                AND COLUMN_IS_VIRTUAL = FALSE
                AND COLUMN_SCHEMA like '']]..SCHEMA_FILTER..[[''
                AND COLUMN_TABLE like '']]..TABLE_FILTER..[[''
                GROUP BY COLUMN_SCHEMA,COLUMN_TABLE
                ') AS partion_keys
)
,vv_table_comments as (
  select 'COMMENT ON TABLE "' || "exa_table_schema" || '"."' || "exa_table_name" || '" IS ''' || REPLACE(table_comment, '''', '''''') || ''';' as sql_text
        from vv_exa_columns where table_comment is not null
        group by "exa_table_schema", "exa_table_name", table_comment
        order by "exa_table_schema", "exa_table_name"
)
,vv_column_comments as (
  select 'COMMENT ON COLUMN "' || "exa_table_schema" || '"."' || "exa_table_name" || '"."' || "exa_column_name" || '" IS ''' || REPLACE(column_comment, '''', '''''') || ''';' as sql_text
        from vv_exa_columns where column_comment is not null
        order by "exa_table_schema", "exa_table_name", ordinal_position
)
, vv_imports as (
  select 'IMPORT INTO "' || "exa_table_schema" || '"."' || "exa_table_name" || '" FROM ]]..CONNECTION_SETTING..[[ AT ]]..CONNECTION_NAME..[[ TABLE "' || table_schema||'"."'||table_name||'";'  as sql_text
	from vv_exa_columns group by "exa_table_schema","exa_table_name", table_schema,table_name
	order by "exa_table_schema","exa_table_name", table_schema,table_name
)]]..enable_cte..view_cte..[[
select SQL_TEXT from (
select 1 as ord, cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
union all
select 2, a.* from vv_create_schemas a
UNION ALL
select 3, cast('-- ### TABLES ###' as varchar(2000000)) SQL_TEXT
union all
select 4, b.* from vv_create_tables b
UNION ALL
select 5, cast('-- ### FOREIGN KEYS ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 6, c.* from vv_create_foreignkey c
UNION ALL
select 7, cast('-- ### PARTITION BY ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 8, d.* from vv_create_partion_key d
UNION ALL
select 9, cast('-- ### DISTRIBUTION KEY ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 10, e.* from vv_create_distribution_key e
UNION ALL
select 11, cast('-- ### COMMENTS ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 12, h.* from vv_table_comments h
UNION ALL
select 13, i.* from vv_column_comments i
UNION ALL
select 14, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select 15, f.* from vv_imports f]]..enable_union..view_union..[[
) order by ord
]],{})

if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return(res)
/

-- EXASOL_TO_EXASOL Data Migration
--
-- You can load data from another Exasol database using the native EXA or JDBC interface; both are built
-- into Exasol (no driver to install). Prefer the native EXA interface: IMPORT FROM EXA is always
-- parallelized, so loading directly from another Exasol database is significantly faster.
--
-- Prerequisites:
-- The other Exasol database and Port 8563 must be reachable from this Exasol database (Firewall settings source and target).
-- The port range from 20000 to 21000 must be opened in the other database if you want to use the native EXA interface (only for Exasol v8 Versions 2025.1.2 and earlier)
-- The user credentials in the connection must be valid.
--
-- JDBC Driver:
-- There is no need to install an Exasol JDBC driver. It is automatically integrated in the product.
--
-- Further documentation: https://docs.exasol.com/db/latest/loading_data/connect_sources/exasol.htm
--
-- Create a connection to the other Exasol database:
-- To create a connection, run one of the following statements.
-- Replace the connection string and credentials as needed.
-- If you use self-signed certificates in your other Exasol database, make sure you add either the fingerprint
-- or the keyword nocertcheck in your connection string.
-- Additionally always make sure you add all your database nodes to the connection string.
-- See also: https://docs.exasol.com/db/latest/connect_exasol/drivers/jdbc.htm
--
-- EXA Connection (fast)
CREATE OR REPLACE CONNECTION EXASOL_EXA
    TO '192.168.6.11..14/nocertcheck:8563'
    USER 'user'
    IDENTIFIED BY 'password';
--
-- To test the connection, run the following statement.
--
SELECT * FROM
(
IMPORT FROM EXA AT EXASOL_EXA
STATEMENT 'SELECT ''Connection works'' '
);
--
--
-- EXA Connection to an Exasol SaaS database (fast)
CREATE OR REPLACE CONNECTION EXASOL_SAAS_EXA
    TO 'my_database_id.clusters.exasol.com:8563'
    USER 'my_user_name'
    IDENTIFIED BY 'my_personal_access_token';
--
-- To test the connection, run the following statement.
--
SELECT * FROM
(
IMPORT FROM EXA AT EXASOL_SAAS_EXA
STATEMENT 'SELECT ''Connection works'' '
);
--
--
-- JDBC Connection (slower than EXA Connection)
CREATE OR REPLACE CONNECTION EXASOL_JDBC
    TO 'jdbc:exa:192.168.6.11..14/nocertcheck:8563'
    USER 'user'
    IDENTIFIED BY 'password';
--
-- To test the connection, run the following statement.
--
SELECT * FROM
(
IMPORT FROM JDBC AT EXASOL_JDBC
STATEMENT 'SELECT ''Connection works'' '
);
--
--
-- JDBC Connection to an Exasol SaaS database (slower than EXA Connection)
CREATE OR REPLACE CONNECTION EXASOL_SAAS_JDBC
    TO 'jdbc:exa:my_database_id.clusters.exasol.com:8563'
    USER 'my_user_name'
    IDENTIFIED BY 'my_personal_access_token';
--
-- To test the connection, run the following statement.
--
SELECT * FROM
(
IMPORT FROM JDBC AT EXASOL_SAAS_JDBC
STATEMENT 'SELECT ''Connection works'' '
);
--
/*
    This script will generate create schema, create table and create import statements
    to load all needed data from an EXASOL database. Automatic datatype conversion is
    applied whenever needed. Copy out the generated statements and execute them in a separate
    SQL Commander window, in the order they are returned.

    Tip: loading is MUCH faster with primary/foreign keys disabled. With PK_SETTING = 'DISABLE'
    (as below) the keys are created disabled and the output ends with an
    "ENABLE PRIMARY & FOREIGN KEYS" section - run that section LAST, after the IMPORTs, to
    (re)validate and activate every key (primary keys first, then foreign keys).
*/
--
EXECUTE SCRIPT DATABASE_MIGRATION.EXASOL_TO_EXASOL(
   'EXASOL_EXA'  -- CONNECTION_NAME: the connection to the SOURCE database
  ,'EXA'         -- CONNECTION_SETTING: 'EXA' (native, parallel, faster) or 'JDBC'
  ,false         -- IDENTIFIER_CASE_INSENSITIVE: false = verbatim/quoted, recommended (preserves lower/MixedCase); true = fold ALL identifiers to UPPER
  ,'%TPCDS_1GB%' -- SCHEMA_FILTER: schema name/filter, '%' = all (SYS, EXA_STATISTICS and virtual schemas are always excluded)
  ,'%'           -- TABLE_FILTER: table name/filter, '%' = all
  ,true          -- GENERATE_VIEWS: true/false, include views (emitted as CREATE OR REPLACE FORCE VIEW)
  ,'%'           -- VIEW_FILTER: view name/filter, '%' = all
  ,'DISABLE'     -- PK_SETTING: 'DISABLE' (faster load; appends an ENABLE-keys section) or 'ENABLE'
  ,'8'           -- TARGET_VERSION: '8' (default) or '7' (downgrade: TIMESTAMP(p) -> TIMESTAMP)
);
