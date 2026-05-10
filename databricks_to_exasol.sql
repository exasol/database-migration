create schema if not exists database_migration;

/*
    This script generates create schema, create table, and import statements
    to load data from Databricks SQL through JDBC.
*/
--/
create or replace script database_migration.DATABRICKS_TO_EXASOL(
    CONNECTION_NAME,                 -- name of the Databricks connection inside Exasol, e.g. databricks_connection
    CATALOG2SCHEMA,                  -- TRUE maps catalog.schema.table to schema catalog and table schema_table
    CATALOG_FILTER,                  -- filter for Databricks catalogs, e.g. 'main', 'ma%', 'main, analytics', '%'
    SCHEMA_FILTER,                   -- filter for Databricks schemas, e.g. 'default', 'sales%', 'bronze, silver', '%'
    TARGET_SCHEMA,                   -- target schema on Exasol side, set to NULL or empty string to use source values
    TABLE_FILTER,                    -- filter for Databricks tables, e.g. 'orders', 'ord%', 'orders, customers', '%'
    IDENTIFIER_CASE_INSENSITIVE      -- TRUE if generated Exasol identifiers should be uppercased
) RETURNS TABLE
AS

local function trim(value)
    return tostring(value):gsub("^%s*(.-)%s*$", "%1")
end

local function is_blank(value)
    return value == null or trim(value) == ''
end

local function sql_literal(value)
    return "'" .. tostring(value):gsub("'", "''") .. "'"
end

local function databricks_literal(value)
    return "'" .. tostring(value):gsub("'", "''") .. "'"
end

local function parse_bool(value, default_value, name)
    if value == null then
        return default_value
    end
    if type(value) == 'boolean' then
        return value
    end

    local text = trim(value):lower()
    if text == '' then
        return default_value
    end
    if text == 'true' or text == '1' or text == 'yes' then
        return true
    end
    if text == 'false' or text == '0' or text == 'no' then
        return false
    end

    error('Invalid boolean for ' .. name .. ': ' .. tostring(value))
end

local function normalize_filter(value)
    if is_blank(value) then
        return '%'
    end
    return trim(value)
end

local function filter_condition(column_name, value)
    local filter_value = normalize_filter(value)
    if filter_value:find(',', 1, true) and not filter_value:find('%%') and not filter_value:find('_') then
        local values = {}
        for part in filter_value:gmatch('[^,]+') do
            values[#values + 1] = databricks_literal(trim(part))
        end
        return column_name .. ' in (' .. table.concat(values, ',') .. ')'
    end
    return column_name .. ' like ' .. databricks_literal(filter_value)
end

if is_blank(CONNECTION_NAME) then
    error('CONNECTION_NAME is required')
end

local catalog2schema = parse_bool(CATALOG2SCHEMA, true, 'CATALOG2SCHEMA')
local identifier_case_insensitive = parse_bool(IDENTIFIER_CASE_INSENSITIVE, true, 'IDENTIFIER_CASE_INSENSITIVE')

local exa_upper_begin = ''
local exa_upper_end = ''
if identifier_case_insensitive then
    exa_upper_begin = 'upper('
    exa_upper_end = ')'
end

local catalog_condition = filter_condition('table_catalog', CATALOG_FILTER)
local schema_condition = filter_condition('table_schema', SCHEMA_FILTER)
local table_condition = filter_condition('table_name', TABLE_FILTER)

local target_schema_expr
local target_table_expr

if not is_blank(TARGET_SCHEMA) then
    target_schema_expr = sql_literal(TARGET_SCHEMA)
    if catalog2schema then
        target_table_expr = [["exa_table_catalog" || '_' || "exa_table_schema" || '_' || "exa_table_name"]]
    else
        target_table_expr = [["exa_table_schema" || '_' || "exa_table_name"]]
    end
elseif catalog2schema then
    target_schema_expr = [["exa_table_catalog"]]
    target_table_expr = [["exa_table_schema" || '_' || "exa_table_name"]]
else
    target_schema_expr = [["exa_table_schema"]]
    target_table_expr = [["exa_table_name"]]
end

local metadata_statement = [[
select
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    case when is_nullable = 'NO' then 'NOT NULL' else 'NULL' end as not_null_constraint,
    data_type,
    full_data_type,
    numeric_precision,
    numeric_scale,
    character_maximum_length
from system.INFORMATION_SCHEMA.COLUMNS
join system.INFORMATION_SCHEMA.TABLES using (table_catalog, table_schema, table_name)
where table_type in ('MANAGED', 'EXTERNAL', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE')
  and ]] .. catalog_condition .. [[
  and ]] .. schema_condition .. [[
  and ]] .. table_condition

local metadata_statement_escaped = metadata_statement:gsub("'", "''")

suc, res = pquery([[
with vv_databricks_columns as (
    select
        ]] .. exa_upper_begin .. [[databricks."table_catalog"]] .. exa_upper_end .. [[ as "exa_table_catalog",
        ]] .. exa_upper_begin .. [[databricks."table_schema"]] .. exa_upper_end .. [[ as "exa_table_schema",
        ]] .. exa_upper_begin .. [[databricks."table_name"]] .. exa_upper_end .. [[ as "exa_table_name",
        ]] .. exa_upper_begin .. [[databricks."column_name"]] .. exa_upper_end .. [[ as "exa_column_name",
        databricks."table_catalog" as table_catalog,
        databricks."table_schema" as table_schema,
        databricks."table_name" as table_name,
        databricks."column_name" as column_name,
        databricks."ordinal_position" as ordinal_position,
        databricks."not_null_constraint" as not_null_constraint,
        databricks."data_type" as data_type,
        databricks."full_data_type" as full_data_type,
        databricks."numeric_precision" as numeric_precision,
        databricks."numeric_scale" as numeric_scale,
        databricks."character_maximum_length" as character_maximum_length
    from (
        IMPORT FROM JDBC DRIVER = 'DATABRICKS' AT ]] .. CONNECTION_NAME .. [[ STATEMENT ']] .. metadata_statement_escaped .. [['
    ) as databricks
),
vv_target_columns as (
    select
        *,
        ]] .. target_schema_expr .. [[ as "target_schema_name",
        ]] .. target_table_expr .. [[ as "target_table_name"
    from vv_databricks_columns
),
vv_create_schemas as (
    select
        'create schema if not exists "' || "target_schema_name" || '";' as sql_text
    from vv_target_columns
    group by "target_schema_name"
    order by "target_schema_name"
),
vv_create_tables as (
    select
        'create or replace table "' || "target_schema_name" || '"."' || "target_table_name" || '" (' ||
        group_concat(
            case
                when upper(data_type) = 'TINYINT' then '"' || "exa_column_name" || '" DECIMAL(3,0) ' || not_null_constraint
                when upper(data_type) = 'SMALLINT' then '"' || "exa_column_name" || '" DECIMAL(5,0) ' || not_null_constraint
                when upper(data_type) = 'INT' then '"' || "exa_column_name" || '" DECIMAL(10,0) ' || not_null_constraint
                when upper(data_type) = 'BIGINT' then '"' || "exa_column_name" || '" DECIMAL(19,0) ' || not_null_constraint
                when upper(data_type) = 'FLOAT' then '"' || "exa_column_name" || '" FLOAT ' || not_null_constraint
                when upper(data_type) = 'DOUBLE' then '"' || "exa_column_name" || '" DOUBLE PRECISION ' || not_null_constraint
                when upper(data_type) = 'DECIMAL' then '"' || "exa_column_name" || '" DECIMAL(' ||
                    case
                        when numeric_precision is null then 36
                        when numeric_precision > 36 then 36
                        else numeric_precision
                    end || ',' ||
                    case
                        when numeric_scale is null then 0
                        when numeric_scale > 36 then 36
                        else numeric_scale
                    end || ') ' || not_null_constraint
                when upper(data_type) = 'BOOLEAN' then '"' || "exa_column_name" || '" BOOLEAN ' || not_null_constraint
                when upper(data_type) = 'DATE' then '"' || "exa_column_name" || '" DATE ' || not_null_constraint
                when upper(data_type) = 'TIMESTAMP' then '"' || "exa_column_name" || '" TIMESTAMP WITH LOCAL TIME ZONE ' || not_null_constraint
                when upper(data_type) = 'TIMESTAMP_LTZ' then '"' || "exa_column_name" || '" TIMESTAMP WITH LOCAL TIME ZONE ' || not_null_constraint
                when upper(data_type) = 'TIMESTAMP_NTZ' then '"' || "exa_column_name" || '" TIMESTAMP ' || not_null_constraint
                when upper(data_type) = 'STRING' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'BINARY' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'ARRAY' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'MAP' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'STRUCT' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'VARIANT' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'OBJECT' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'INTERVAL' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'VOID' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'GEOGRAPHY' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
                when upper(data_type) = 'GEOMETRY' then '"' || "exa_column_name" || '" VARCHAR(2000000) ' || not_null_constraint
            end
            order by ordinal_position separator ','
        ) || ');' ||
        coalesce(
            group_concat(
                case
                    when upper(data_type) in ('BINARY', 'ARRAY', 'MAP', 'STRUCT', 'VARIANT', 'OBJECT', 'INTERVAL', 'VOID', 'GEOGRAPHY', 'GEOMETRY')
                    then '--LOSSY_DATATYPE: "' || "exa_column_name" || '" Databricks TYPE INFO: ' || full_data_type
                    when upper(data_type) not in ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'STRING', 'BINARY', 'ARRAY', 'MAP', 'STRUCT', 'VARIANT', 'OBJECT', 'INTERVAL', 'VOID', 'GEOGRAPHY', 'GEOMETRY')
                    then '--UNKNOWN_DATATYPE: "' || "exa_column_name" || '" Databricks TYPE INFO: ' || full_data_type
                end
                order by ordinal_position separator ' '
            ),
            ''
        ) as sql_text
    from vv_target_columns
    group by "target_schema_name", "target_table_name"
),
vv_imports as (
    select
        'import into "' || "target_schema_name" || '"."' || "target_table_name" || '"(' ||
        group_concat(
            case
                when upper(data_type) in ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'STRING', 'BINARY', 'ARRAY', 'MAP', 'STRUCT', 'VARIANT', 'OBJECT', 'INTERVAL', 'VOID', 'GEOGRAPHY', 'GEOMETRY')
                then '"' || "exa_column_name" || '"'
            end
            order by ordinal_position separator ','
        ) || ') from jdbc driver = ''DATABRICKS'' at ]] .. CONNECTION_NAME .. [[ statement ''select ' ||
        group_concat(
            case
                when upper(data_type) in ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'STRING')
                then '`' || column_name || '`'
                when upper(data_type) in ('BINARY', 'ARRAY', 'MAP', 'STRUCT', 'VARIANT', 'OBJECT', 'INTERVAL', 'VOID', 'GEOGRAPHY', 'GEOMETRY')
                then 'cast(`' || column_name || '` as string)'
            end
            order by ordinal_position separator ','
        ) || ' from `' || table_catalog || '`.`' || table_schema || '`.`' || table_name || '`'';' as sql_text
    from vv_target_columns
    group by "target_schema_name", "target_table_name", table_catalog, table_schema, table_name
)
select sql_text from (
    select 1 as ord, cast('-- ### SCHEMAS ###' as varchar(2000000)) as sql_text
    union all
    select 2, sql_text from vv_create_schemas
    union all
    select 3, cast('-- ### TABLES ###' as varchar(2000000)) as sql_text
    union all
    select 4, sql_text from vv_create_tables where sql_text is not null and sql_text not like '%();%'
    union all
    select 5, cast('-- ### IMPORTS ###' as varchar(2000000)) as sql_text
    union all
    select 6, sql_text from vv_imports where sql_text is not null and sql_text not like '%select  from%'
) order by ord, sql_text
]], {})

if not suc then
    local error_message = 'unknown error'
    local statement_text = metadata_statement
    if res ~= null then
        error_message = res.error_message or error_message
        statement_text = res.statement_text or statement_text
    end
    error('"' .. error_message .. '" Caught while executing: "' .. statement_text .. '"')
end

return res
/

-- Create a connection to Databricks
CREATE OR REPLACE CONNECTION DATABRICKS_CONNECTION TO
  'jdbc:databricks://<server-hostname>:443/default;httpPath=<http-path>;AuthMech=3;UseNativeQuery=1'
  USER 'token' IDENTIFIED BY '<personal-access-token>';

-- Finally start the import process
execute script database_migration.DATABRICKS_TO_EXASOL(
    'DATABRICKS_CONNECTION', -- CONNECTION_NAME: name of the Databricks connection inside Exasol
    true,                    -- CATALOG2SCHEMA: TRUE maps catalog.schema.table to catalog.schema_table
    '%',                     -- CATALOG_FILTER: '%' to load all visible catalogs
    '%',                     -- SCHEMA_FILTER: '%' to load all visible schemas
    NULL,                    -- TARGET_SCHEMA: NULL or empty string to use source-derived schema names
    '%',                     -- TABLE_FILTER: '%' to load all visible base tables
    true                     -- IDENTIFIER_CASE_INSENSITIVE: TRUE uppercases generated Exasol identifiers
);
