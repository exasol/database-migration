create schema if not exists database_migration;

/*
    Unified entry point for database migration scripts.

    This script keeps the existing source-specific scripts intact and exposes one
    standardized call surface. It dispatches to the current adapter script, then
    either returns generated SQL (DEBUG = TRUE) or executes generated SQL
    (DEBUG = FALSE).

    Source-specific settings live in OPTIONS as key=value pairs separated by ';'.
*/
--/
create or replace script database_migration.MIGRATE_TO_EXASOL(
    SOURCE_TYPE,                  -- migration source, e.g. MYSQL, POSTGRES, DATABRICKS, SNOWFLAKE
    CONNECTION_NAME,              -- name of the source database connection inside Exasol
    CONNECTION_TYPE,              -- Exasol-to-Exasol only: JDBC or EXA
    DB_FILTER,                    -- database/catalog filter where supported, else '%'
    SCHEMA_FILTER,                -- schema filter, e.g. 'MY_SCHEMA', 'MART_%', or '%'
    TABLE_FILTER,                 -- table filter, e.g. 'MY_TABLE', 'FACT_%', or '%'
    TARGET_SCHEMA,                -- target schema override where supported, or NULL
    IDENTIFIER_CASE_INSENSITIVE,  -- TRUE stores generated identifiers uppercase
    DEBUG,                        -- TRUE previews generated SQL, FALSE executes it
    OPTIONS                       -- source-specific KEY=VALUE pairs separated by semicolons
) RETURNS TABLE
AS

local OUT_COLUMNS = "STEP_KIND VARCHAR(40), TARGET_OBJ VARCHAR(2000), ROWS_AFFECTED DECIMAL(18,0), ELAPSED_MS DECIMAL(18,0), RESULT_FLAG VARCHAR(20), SQL_TEXT VARCHAR(2000000), ERROR_MESSAGE VARCHAR(20000)"

function is_null(value)
    return value == nil or value == null or value == NULL
end

function trim(value)
    if is_null(value) then
        return nil
    end
    return tostring(value):gsub("^%s*(.-)%s*$", "%1")
end

function blank_to_nil(value)
    local result = trim(value)
    if result == nil or result == '' then
        return nil
    end
    return result
end

function escape_sql_literal(value)
    return tostring(value):gsub("'", "''")
end

function sql_string(value)
    local result = blank_to_nil(value)
    if result == nil then
        return "NULL"
    end
    return "'" .. escape_sql_literal(result) .. "'"
end

function sql_bool(value)
    if value then
        return "TRUE"
    end
    return "FALSE"
end

function parse_bool(value, default_value, param_name)
    if is_null(value) or blank_to_nil(value) == nil then
        return default_value
    end
    if type(value) == 'boolean' then
        return value
    end

    local normalized = string.upper(trim(value))
    if normalized == 'TRUE' or normalized == 'T' or normalized == 'YES' or normalized == 'Y' or normalized == '1' then
        return true
    elseif normalized == 'FALSE' or normalized == 'F' or normalized == 'NO' or normalized == 'N' or normalized == '0' then
        return false
    end

    error('Invalid boolean for ' .. param_name .. ': ' .. tostring(value))
end

function parse_options(raw_options)
    local parsed = {}
    local raw = blank_to_nil(raw_options)
    if raw == nil then
        return parsed
    end

    for entry in string.gmatch(raw, "([^;]+)") do
        local key, value = entry:match("^%s*([^=]+)%s*=%s*(.-)%s*$")
        if key == nil then
            error('Invalid OPTIONS entry: ' .. entry .. '. Use KEY=VALUE pairs separated by semicolons.')
        end
        parsed[string.upper(trim(key))] = trim(value)
    end

    return parsed
end

function opt(options, key, default_value)
    local value = options[string.upper(key)]
    if blank_to_nil(value) == nil then
        return default_value
    end
    return value
end

function opt_bool(options, key, default_value)
    return parse_bool(opt(options, key, nil), default_value, key)
end

function opt_sql_number(options, key, default_value)
    local value = opt(options, key, default_value)
    if blank_to_nil(value) == nil then
        return "NULL"
    end
    local number_value = tonumber(value)
    if number_value == nil then
        error('Invalid numeric option ' .. key .. ': ' .. tostring(value))
    end
    return tostring(number_value)
end

function normalize_source_type(source_type)
    local source = string.upper(blank_to_nil(source_type) or '')
    source = source:gsub("%s+", "_"):gsub("-", "_")

    if source == 'AZURESQL' or source == 'AZURE_SQL_SERVER' then
        return 'AZURE_SQL'
    elseif source == 'BIG_QUERY' or source == 'GOOGLE_BIGQUERY' or source == 'GOOGLE_BIG_QUERY' then
        return 'BIGQUERY'
    elseif source == 'DBX' or source == 'DATABRICKS_SQL' then
        return 'DATABRICKS'
    elseif source == 'POSTGRESQL' then
        return 'POSTGRES'
    elseif source == 'SQL_SERVER' or source == 'MSSQL' or source == 'MICROSOFT_SQL_SERVER' then
        return 'SQLSERVER'
    elseif source == 'SAP_HANA' or source == 'SAPHANA' then
        return 'HANA'
    elseif source == 'EXA' then
        return 'EXASOL'
    elseif source == 'NZ' then
        return 'NETEZZA'
    elseif source == 'ACTIAN' or source == 'ACTIAN_VECTOR' then
        return 'VECTORWISE'
    end

    return source
end

function require_value(value, name)
    local result = blank_to_nil(value)
    if result == nil then
        error(name .. ' is required')
    end
    return result
end

function first_sql_text(row)
    if row.SQL_TEXT ~= nil then
        return row.SQL_TEXT
    end
    if row[1] ~= nil then
        return row[1]
    end
    return tostring(row)
end

function classify_step(sql_text)
    local text = blank_to_nil(sql_text)
    if text == nil then
        return 'INFO'
    end
    if string.sub(text, 1, 2) == '--' then
        return 'INFO'
    end
    local lower = string.lower(text)
    if string.find(lower, '^%s*create%s+schema') then
        return 'CREATE_SCHEMA'
    elseif string.find(lower, '^%s*create%s+or%s+replace%s+table')
        or string.find(lower, '^%s*create%s+table') then
        return 'CREATE_TABLE'
    elseif string.find(lower, '^%s*alter%s+table') then
        return 'ALTER_TABLE'
    elseif string.find(lower, '^%s*import%s+into') then
        return 'IMPORT'
    elseif string.find(lower, '^%s*insert%s+into') then
        return 'IMPORT'
    end
    return 'OTHER'
end

function extract_target_obj(sql_text, step_kind)
    local text = blank_to_nil(sql_text)
    if text == nil then
        return NULL
    end
    if step_kind == 'CREATE_SCHEMA' then
        local schema = text:match('[Cc][Rr][Ee][Aa][Tt][Ee]%s+[Ss][Cc][Hh][Ee][Mm][Aa]%s+[Ii][Ff]%s+[Nn][Oo][Tt]%s+[Ee][Xx][Ii][Ss][Tt][Ss]%s+"([^"]+)"')
            or text:match('[Cc][Rr][Ee][Aa][Tt][Ee]%s+[Ss][Cc][Hh][Ee][Mm][Aa]%s+"([^"]+)"')
            or text:match('[Cc][Rr][Ee][Aa][Tt][Ee]%s+[Ss][Cc][Hh][Ee][Mm][Aa]%s+([%w_]+)')
        if schema then return schema end
    elseif step_kind == 'CREATE_TABLE' or step_kind == 'ALTER_TABLE' or step_kind == 'IMPORT' then
        local schema, table_name = text:match('"([^"]+)"%."([^"]+)"')
        if schema and table_name then return schema .. '.' .. table_name end
    end
    return NULL
end

function is_executable_statement(sql_text)
    local text = blank_to_nil(sql_text)
    if text == nil then
        return false
    end
    return string.sub(text, 1, 2) ~= '--'
end

function build_row(step_kind, target_obj, rows_affected, elapsed_ms, result_flag, sql_text, error_message)
    return {step_kind, target_obj, rows_affected, elapsed_ms, result_flag, sql_text, error_message}
end

function normalize_rows(res)
    local summary = {}
    local tables_count = 0
    for i = 1, #res do
        local row = res[i]
        local sql_text = first_sql_text(row)
        local kind = classify_step(sql_text)
        local target = extract_target_obj(sql_text, kind)
        if kind == 'CREATE_TABLE' then
            tables_count = tables_count + 1
        end
        local flag = row.RESULT_FLAG or row.SUCCESS or row[5] or row[2] or 'PREVIEW'
        local err = row.ERROR_MESSAGE or row[7] or row[3] or NULL
        summary[#summary + 1] = build_row(kind, target, NULL, NULL, flag, sql_text, err)
    end
    summary[#summary + 1] = build_row('SUMMARY', 'Plan: ' .. tables_count .. ' table(s) to create', NULL, NULL, 'PREVIEW', NULL, NULL)
    return summary
end

function execute_generated_sql(res)
    local summary = {}
    local fail_count = 0
    local executed_count = 0
    local tables_created = 0
    local total_rows = 0
    local total_elapsed = 0

    for i = 1, #res do
        local sql_text = first_sql_text(res[i])
        local kind = classify_step(sql_text)
        local target = extract_target_obj(sql_text, kind)
        if is_executable_statement(sql_text) then
            executed_count = executed_count + 1
            local t0 = os.clock()
            local success, info = pquery(sql_text)
            local elapsed = math.floor((os.clock() - t0) * 1000)
            total_elapsed = total_elapsed + elapsed
            if success then
                local rows_affected = NULL
                if info ~= nil and info.rows_affected ~= nil then
                    rows_affected = tonumber(info.rows_affected) or NULL
                    if kind == 'IMPORT' and type(rows_affected) == 'number' then
                        total_rows = total_rows + rows_affected
                    end
                end
                if kind == 'CREATE_TABLE' then
                    tables_created = tables_created + 1
                end
                summary[#summary + 1] = build_row(kind, target, rows_affected, elapsed, 'OK', sql_text, NULL)
            else
                fail_count = fail_count + 1
                summary[#summary + 1] = build_row(kind, target, NULL, elapsed, 'ERROR', sql_text, info.error_message)
            end
        else
            summary[#summary + 1] = build_row(kind, target, NULL, NULL, 'SKIPPED', sql_text, NULL)
        end
    end

    local summary_obj
    local summary_flag
    if executed_count == 0 then
        summary_obj = 'No executable SQL generated'
        summary_flag = 'SKIPPED'
    elseif fail_count == 0 then
        summary_obj = 'Completed: ' .. tables_created .. ' table(s), ' .. total_rows .. ' row(s) loaded'
        summary_flag = 'OK'
    else
        summary_obj = 'Completed with ' .. fail_count .. ' error(s); ' .. tables_created .. ' table(s) created, ' .. total_rows .. ' row(s) loaded'
        summary_flag = 'ERROR'
    end
    summary[#summary + 1] = build_row('SUMMARY', summary_obj, total_rows, total_elapsed, summary_flag, NULL, NULL)

    return summary
end

-- Per-source metadata SQL. Each template returns 8 columns in this order:
--   src_schema, src_table, src_rows,
--   src_pk_col, src_pk_type, src_date_col, src_num_col, src_partitioned
-- Tier 0 sources (POSTGRES, MYSQL, SQLSERVER, SNOWFLAKE) populate all eight columns.
-- Other sources currently emit only src_rows; splitter falls through to ROWID
-- or SINGLE depending on dialect capabilities.
-- BIGQUERY remains skip_with_info: BQ INFORMATION_SCHEMA is dataset-scoped, and
-- the cross-dataset round-trip needs per-dataset query orchestration not yet built.
SOURCE_METADATA_BY_SOURCE = {
    ORACLE = {
        mode = 'sql',
        template = "select owner, table_name, num_rows, NULL, NULL, NULL, NULL, NULL from all_tables where (<PREDICATE>)",
        pair = "(owner = '%s' and table_name = '%s')",
    },
    POSTGRES = {
        mode = 'sql',
        template = "select n.nspname, c.relname, c.reltuples::bigint,"
            .. " (select a.attname::text from pg_constraint con join pg_attribute a on a.attrelid = con.conrelid and a.attnum = con.conkey[1] where con.conrelid = c.oid and con.contype = 'p' and array_length(con.conkey, 1) = 1 and a.atttypid in (20, 21, 23, 700, 701, 1700) limit 1),"
            .. " (select format_type(a.atttypid, a.atttypmod) from pg_constraint con join pg_attribute a on a.attrelid = con.conrelid and a.attnum = con.conkey[1] where con.conrelid = c.oid and con.contype = 'p' and array_length(con.conkey, 1) = 1 and a.atttypid in (20, 21, 23, 700, 701, 1700) limit 1),"
            .. " (select a.attname::text from pg_attribute a where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped and a.atttypid in (1082, 1114, 1184) order by (case when a.attname ~* '(date|dt|time|day|created|loaded|event|posted)$' then 0 else 1 end), a.attnum limit 1),"
            .. " (select a.attname::text from pg_attribute a where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped and a.attnotnull and a.atttypid in (20, 21, 23, 700, 701, 1700) order by a.attnum limit 1),"
            .. " (c.relkind = 'p')"
            .. " from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r','p') and (<PREDICATE>)",
        pair = "(n.nspname = '%s' and c.relname = '%s')",
    },
    MYSQL = {
        mode = 'sql',
        template = "select t.table_schema, t.table_name, t.table_rows,"
            .. " (select kcu.column_name from information_schema.key_column_usage kcu join information_schema.table_constraints tc on tc.constraint_name = kcu.constraint_name and tc.table_schema = kcu.table_schema and tc.table_name = kcu.table_name join information_schema.columns col on col.table_schema = kcu.table_schema and col.table_name = kcu.table_name and col.column_name = kcu.column_name where tc.constraint_type = 'PRIMARY KEY' and kcu.table_schema = t.table_schema and kcu.table_name = t.table_name and col.data_type in ('tinyint','smallint','mediumint','int','bigint','decimal','numeric','float','double') and kcu.constraint_name in (select constraint_name from information_schema.key_column_usage where table_schema = t.table_schema and table_name = t.table_name group by constraint_name having count(*) = 1) limit 1),"
            .. " (select col.data_type from information_schema.columns col where col.table_schema = t.table_schema and col.table_name = t.table_name and col.column_name = (select kcu.column_name from information_schema.key_column_usage kcu join information_schema.table_constraints tc on tc.constraint_name = kcu.constraint_name and tc.table_schema = kcu.table_schema and tc.table_name = kcu.table_name where tc.constraint_type = 'PRIMARY KEY' and kcu.table_schema = t.table_schema and kcu.table_name = t.table_name limit 1) limit 1),"
            .. " (select column_name from information_schema.columns where table_schema = t.table_schema and table_name = t.table_name and data_type in ('date','datetime','timestamp') order by (case when lower(column_name) regexp '(date|dt|time|day|created|loaded|event|posted)$' then 0 else 1 end), ordinal_position limit 1),"
            .. " (select column_name from information_schema.columns where table_schema = t.table_schema and table_name = t.table_name and is_nullable = 'NO' and data_type in ('tinyint','smallint','mediumint','int','bigint','decimal','numeric','float','double') order by ordinal_position limit 1),"
            .. " (select count(*) > 0 from information_schema.partitions where table_schema = t.table_schema and table_name = t.table_name and partition_name is not null)"
            .. " from information_schema.tables t where (<PREDICATE>)",
        pair = "(t.table_schema = '%s' and t.table_name = '%s')",
    },
    SQLSERVER = {
        mode = 'sql',
        template = "select s.name as src_schema, t.name as src_table, sum(ps.row_count) as src_rows,"
            .. " (select top 1 c2.name from sys.indexes i join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id join sys.columns c2 on c2.object_id = ic.object_id and c2.column_id = ic.column_id join sys.types ty on ty.user_type_id = c2.user_type_id where i.object_id = t.object_id and i.is_primary_key = 1 and ty.name in ('tinyint','smallint','int','bigint','decimal','numeric','float','real','money','smallmoney') and (select count(*) from sys.index_columns ic2 where ic2.object_id = i.object_id and ic2.index_id = i.index_id) = 1) as src_pk_col,"
            .. " (select top 1 ty.name from sys.indexes i join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id join sys.columns c2 on c2.object_id = ic.object_id and c2.column_id = ic.column_id join sys.types ty on ty.user_type_id = c2.user_type_id where i.object_id = t.object_id and i.is_primary_key = 1 and ty.name in ('tinyint','smallint','int','bigint','decimal','numeric','float','real','money','smallmoney') and (select count(*) from sys.index_columns ic2 where ic2.object_id = i.object_id and ic2.index_id = i.index_id) = 1) as src_pk_type,"
            .. " (select top 1 c2.name from sys.columns c2 join sys.types ty on ty.user_type_id = c2.user_type_id where c2.object_id = t.object_id and ty.name in ('date','datetime','datetime2','smalldatetime','datetimeoffset','time') order by (case when lower(c2.name) like '%date' or lower(c2.name) like '%dt' or lower(c2.name) like '%time' or lower(c2.name) like '%day' or lower(c2.name) like '%created' or lower(c2.name) like '%loaded' or lower(c2.name) like '%event' or lower(c2.name) like '%posted' then 0 else 1 end), c2.column_id) as src_date_col,"
            .. " (select top 1 c2.name from sys.columns c2 join sys.types ty on ty.user_type_id = c2.user_type_id where c2.object_id = t.object_id and c2.is_nullable = 0 and ty.name in ('tinyint','smallint','int','bigint','decimal','numeric','float','real','money','smallmoney') order by c2.column_id) as src_num_col,"
            .. " (case when exists(select 1 from sys.partitions p where p.object_id = t.object_id and p.partition_number > 1) then 1 else 0 end) as src_partitioned"
            .. " from sys.tables t join sys.schemas s on s.schema_id = t.schema_id join sys.dm_db_partition_stats ps on ps.object_id = t.object_id and ps.index_id in (0, 1) where (<PREDICATE>) group by s.name, t.name, t.object_id",
        pair = "(s.name = '%s' and t.name = '%s')",
    },
    SNOWFLAKE = {
        mode = 'sql',
        template = "select t.table_schema, t.table_name, t.row_count,"
            .. " (select kcu.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kcu on kcu.constraint_name = tc.constraint_name and kcu.table_schema = tc.table_schema and kcu.table_name = tc.table_name join information_schema.columns col on col.table_schema = kcu.table_schema and col.table_name = kcu.table_name and col.column_name = kcu.column_name where tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = t.table_schema and tc.table_name = t.table_name and col.data_type in ('NUMBER','DECIMAL','FLOAT','REAL','DOUBLE','INTEGER','BIGINT','SMALLINT','TINYINT','BYTEINT') and (select count(*) from information_schema.key_column_usage k2 where k2.constraint_name = tc.constraint_name and k2.table_schema = tc.table_schema and k2.table_name = tc.table_name) = 1 limit 1) as src_pk_col,"
            .. " (select col.data_type from information_schema.columns col where col.table_schema = t.table_schema and col.table_name = t.table_name and col.column_name = (select kcu.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kcu on kcu.constraint_name = tc.constraint_name and kcu.table_schema = tc.table_schema and kcu.table_name = tc.table_name where tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = t.table_schema and tc.table_name = t.table_name limit 1) limit 1) as src_pk_type,"
            .. " (select column_name from information_schema.columns where table_schema = t.table_schema and table_name = t.table_name and data_type in ('DATE','TIMESTAMP','TIMESTAMP_LTZ','TIMESTAMP_NTZ','TIMESTAMP_TZ','DATETIME','TIME') order by (case when lower(column_name) regexp '(date|dt|time|day|created|loaded|event|posted)$' then 0 else 1 end), ordinal_position limit 1) as src_date_col,"
            .. " (select column_name from information_schema.columns where table_schema = t.table_schema and table_name = t.table_name and is_nullable = 'NO' and data_type in ('NUMBER','DECIMAL','FLOAT','REAL','DOUBLE','INTEGER','BIGINT','SMALLINT','TINYINT','BYTEINT') order by ordinal_position limit 1) as src_num_col,"
            .. " FALSE as src_partitioned"
            .. " from information_schema.tables t where (<PREDICATE>)",
        pair = "(t.table_schema = '%s' and t.table_name = '%s')",
    },
    BIGQUERY = {
        mode = 'skip_with_info',
        reason = 'BigQuery INFORMATION_SCHEMA is dataset-scoped; per-dataset metadata round-trip not yet implemented',
    },
    REDSHIFT = {
        mode = 'sql',
        template = [[select "schema", "table", tbl_rows, NULL, NULL, NULL, NULL, NULL from svv_table_info where (<PREDICATE>)]],
        pair = [[("schema" = '%s' and "table" = '%s')]],
    },
    VERTICA = {
        mode = 'sql',
        template = "select projection_schema, anchor_table_name, row_count, NULL, NULL, NULL, NULL, NULL from projection_storage where (<PREDICATE>)",
        pair = "(projection_schema = '%s' and anchor_table_name = '%s')",
    },
    DB2 = {
        mode = 'sql',
        template = "select tabschema, tabname, card, NULL, NULL, NULL, NULL, NULL from syscat.tables where (<PREDICATE>)",
        pair = "(tabschema = '%s' and tabname = '%s')",
    },
    HANA = {
        mode = 'sql',
        template = "select schema_name, table_name, record_count, NULL, NULL, NULL, NULL, NULL from sys.m_tables where (<PREDICATE>)",
        pair = "(schema_name = '%s' and table_name = '%s')",
    },
    NETEZZA = {
        mode = 'sql',
        template = [[select schema, tablename, reltuples, NULL, NULL, NULL, NULL, NULL from _v_table where (<PREDICATE>)]],
        pair = [[(schema = '%s' and tablename = '%s')]],
    },
    TERADATA = {
        mode = 'sql',
        template = "select databasename, tablename, currentpermspace, NULL, NULL, NULL, NULL, NULL from dbc.tablesizev where (<PREDICATE>)",
        pair = "(databasename = '%s' and tablename = '%s')",
    },
    DATABRICKS = {
        mode = 'sql',
        template = "select table_schema, table_name, cast(null as bigint) as row_count, NULL, NULL, NULL, NULL, NULL from information_schema.tables where (<PREDICATE>)",
        pair = "(table_schema = '%s' and table_name = '%s')",
    },
}
SOURCE_METADATA_BY_SOURCE.MARIADB = SOURCE_METADATA_BY_SOURCE.MYSQL
SOURCE_METADATA_BY_SOURCE.AZURE_SQL = SOURCE_METADATA_BY_SOURCE.SQLSERVER

-- Per-source SQL-dialect building blocks consumed by transform_for_split.
-- Each entry exposes pushdown-friendly fragments for date bucketing, hash
-- bucketing, and (where supported) ROWID-range bucketing. Sources missing an
-- entry fall through to SINGLE in the split hierarchy.
DIALECT_BY_SOURCE = {
    POSTGRES = {
        month_fn = function(col) return 'EXTRACT(MONTH FROM "' .. col .. '")' end,
        day_fn = function(col) return 'EXTRACT(DAY FROM "' .. col .. '")' end,
        year_month_fn = function(col) return '(EXTRACT(YEAR FROM "' .. col .. '") * 12 + EXTRACT(MONTH FROM "' .. col .. '"))' end,
        hash_where = function(col, n, k) return 'MOD(ABS(HASHTEXT("' .. col .. '"::text)), ' .. n .. ') = ' .. k end,
        rowid_supported = true,
        rowid_expr = 'ctid',
        rowid_where = function(n, k) return 'MOD(ABS(HASHTEXT(ctid::text)), ' .. n .. ') = ' .. k end,
    },
    SQLSERVER = {
        month_fn = function(col) return 'MONTH("' .. col .. '")' end,
        day_fn = function(col) return 'DAY("' .. col .. '")' end,
        year_month_fn = function(col) return '(YEAR("' .. col .. '") * 12 + MONTH("' .. col .. '"))' end,
        hash_where = function(col, n, k) return '(ABS(CHECKSUM("' .. col .. '")) % ' .. n .. ') = ' .. k end,
        rowid_supported = true,
        rowid_expr = '%%physloc%%',
        rowid_where = function(n, k) return '(ABS(CHECKSUM(%%physloc%%)) % ' .. n .. ') = ' .. k end,
    },
    MYSQL = {
        month_fn = function(col) return 'MONTH("' .. col .. '")' end,
        day_fn = function(col) return 'DAY("' .. col .. '")' end,
        year_month_fn = function(col) return '(YEAR("' .. col .. '") * 12 + MONTH("' .. col .. '"))' end,
        hash_where = function(col, n, k) return '(CONV(SUBSTRING(MD5(CAST("' .. col .. '" AS CHAR)), 1, 8), 16, 10) MOD ' .. n .. ') = ' .. k end,
        rowid_supported = false,
    },
    SNOWFLAKE = {
        month_fn = function(col) return 'MONTH("' .. col .. '")' end,
        day_fn = function(col) return 'DAY("' .. col .. '")' end,
        year_month_fn = function(col) return '(YEAR("' .. col .. '") * 12 + MONTH("' .. col .. '"))' end,
        hash_where = function(col, n, k) return '(ABS(HASH("' .. col .. '")) % ' .. n .. ') = ' .. k end,
        rowid_supported = false,
    },
    ORACLE = {
        month_fn = function(col) return 'EXTRACT(MONTH FROM "' .. col .. '")' end,
        day_fn = function(col) return 'EXTRACT(DAY FROM "' .. col .. '")' end,
        year_month_fn = function(col) return '(EXTRACT(YEAR FROM "' .. col .. '") * 12 + EXTRACT(MONTH FROM "' .. col .. '"))' end,
        hash_where = function(col, n, k) return 'MOD(ORA_HASH("' .. col .. '"), ' .. n .. ') = ' .. k end,
        rowid_supported = true,
        rowid_expr = 'ROWID',
        rowid_where = function(n, k) return 'MOD(ORA_HASH(ROWID), ' .. n .. ') = ' .. k end,
    },
    DB2 = {
        month_fn = function(col) return 'MONTH("' .. col .. '")' end,
        day_fn = function(col) return 'DAY("' .. col .. '")' end,
        year_month_fn = function(col) return '(YEAR("' .. col .. '") * 12 + MONTH("' .. col .. '"))' end,
        hash_where = function(col, n, k) return 'MOD(HASH4("' .. col .. '"), ' .. n .. ') = ' .. k end,
        rowid_supported = true,
        rowid_expr = 'RID_BIT(t)',
        rowid_where = function(n, k) return 'MOD(HASH4(RID_BIT(t)), ' .. n .. ') = ' .. k end,
    },
    VERTICA = {
        month_fn = function(col) return 'MONTH("' .. col .. '")' end,
        day_fn = function(col) return 'DAY("' .. col .. '")' end,
        year_month_fn = function(col) return '(YEAR("' .. col .. '") * 12 + MONTH("' .. col .. '"))' end,
        hash_where = function(col, n, k) return 'MOD(HASH("' .. col .. '"), ' .. n .. ') = ' .. k end,
        rowid_supported = false,
    },
    HANA = {
        month_fn = function(col) return 'MONTH("' .. col .. '")' end,
        day_fn = function(col) return 'DAYOFMONTH("' .. col .. '")' end,
        year_month_fn = function(col) return '(YEAR("' .. col .. '") * 12 + MONTH("' .. col .. '"))' end,
        hash_where = function(col, n, k) return 'MOD(HASH_SHA256("' .. col .. '"), ' .. n .. ') = ' .. k end,
        rowid_supported = false,
    },
    REDSHIFT = {
        month_fn = function(col) return 'EXTRACT(MONTH FROM "' .. col .. '")' end,
        day_fn = function(col) return 'EXTRACT(DAY FROM "' .. col .. '")' end,
        year_month_fn = function(col) return '(EXTRACT(YEAR FROM "' .. col .. '") * 12 + EXTRACT(MONTH FROM "' .. col .. '"))' end,
        hash_where = function(col, n, k) return '(STRTOL(SUBSTRING(MD5("' .. col .. '"::varchar), 1, 8), 16) % ' .. n .. ') = ' .. k end,
        rowid_supported = false,
    },
    DATABRICKS = {
        month_fn = function(col) return 'MONTH(`' .. col .. '`)' end,
        day_fn = function(col) return 'DAY(`' .. col .. '`)' end,
        year_month_fn = function(col) return '(YEAR(`' .. col .. '`) * 12 + MONTH(`' .. col .. '`))' end,
        hash_where = function(col, n, k) return 'PMOD(HASH(`' .. col .. '`), ' .. n .. ') = ' .. k end,
        rowid_supported = false,
    },
}
DIALECT_BY_SOURCE.MARIADB = DIALECT_BY_SOURCE.MYSQL
DIALECT_BY_SOURCE.AZURE_SQL = DIALECT_BY_SOURCE.SQLSERVER

function count_statement_clauses(sql)
    if sql == nil then return 0 end
    local n = 0
    for _ in string.gmatch(sql, "[Ss][Tt][Aa][Tt][Ee][Mm][Ee][Nn][Tt]%s+'") do
        n = n + 1
    end
    return n
end

function extract_source_ref_from_import(sql)
    if sql == nil then return nil, nil end
    local schema, table_name = sql:match('[Ff][Rr][Oo][Mm]%s+"([^"]+)"%."([^"]+)"')
    return schema, table_name
end

function rewrite_to_first_statement(sql)
    if sql == nil then return sql end
    local stmt_kw_start = string.find(sql:lower(), "statement%s+'")
    if not stmt_kw_start then return sql end
    local quote_open = sql:find("'", stmt_kw_start)
    if not quote_open then return sql end
    local i = quote_open + 1
    while i <= #sql do
        local c = sql:sub(i, i)
        if c == "'" then
            if sql:sub(i + 1, i + 1) == "'" then
                i = i + 2
            else
                return sql:sub(1, i)
            end
        else
            i = i + 1
        end
    end
    return sql
end

function replace_row_sql(row, new_sql)
    return { SQL_TEXT = new_sql, [1] = new_sql }
end

function parse_threshold(options)
    local raw = opt(options, 'PARALLEL_ROW_THRESHOLD', '1000000')
    local n = tonumber(raw)
    if n == nil then
        error('Invalid numeric option PARALLEL_ROW_THRESHOLD: ' .. tostring(raw))
    end
    return n
end

-- Collects unique source (schema, table) pairs the metadata round-trip needs.
-- Multi-statement IMPORTs are needed by the gate (Speq 1) for threshold-collapse.
-- Single-statement IMPORTs are needed by the splitter (Speq 2) for expansion.
-- A single round-trip serves both consumers; the cache is keyed on source ident.
-- Skipped entirely when threshold = 0 AND splitter is OFF AND PARALLEL_STATEMENTS = 1.
function collect_metadata_pairs(res, options)
    local pair_list = {}
    local pairs_seen = {}
    if res == nil then return pair_list end

    local threshold = parse_threshold(options)
    local split_directive_ok, split_directive = pcall(parse_split_directive, options)
    local split_off = split_directive_ok and split_directive.mode == 'OFF'
    local resolved_n = resolve_split_n(options, nil)
    local splitter_disabled = split_off or resolved_n < 2

    if threshold <= 0 and splitter_disabled then return pair_list end

    for i = 1, #res do
        local sql_text = first_sql_text(res[i])
        if classify_step(sql_text) == 'IMPORT' then
            local clauses = count_statement_clauses(sql_text)
            local relevant = false
            if clauses > 1 and threshold > 0 then
                relevant = true
            elseif clauses == 1 and not splitter_disabled and threshold > 0 then
                relevant = true
            end
            if relevant then
                local src_schema, src_table = extract_source_ref_from_import(sql_text)
                if src_schema ~= nil and src_table ~= nil then
                    local key = src_schema .. '\t' .. src_table
                    if not pairs_seen[key] then
                        pairs_seen[key] = true
                        pair_list[#pair_list + 1] = { schema = src_schema, table_name = src_table }
                    end
                end
            end
        end
    end
    return pair_list
end

-- Returns a metadata cache describing every source table referenced by IMPORTs in `res`.
-- Shape:
--   { available = bool, rows = { ["schema\ttable"] = {src_rows, src_pk_col, ...} },
--     info_rows = { "-- ..." } }
-- `available = false` means downstream consumers MUST pass IMPORTs through unchanged
-- (Speq 2's soft-fail invariant for the splitter; matches Speq 1's gate behavior for
-- lookup failure / unsupported source type).
function transform_for_metadata(res, source_type, connection_name, options)
    local empty = { available = false, rows = {}, info_rows = {} }
    if res == nil or #res == 0 then return empty end

    local pair_list = collect_metadata_pairs(res, options)
    if #pair_list == 0 then return empty end

    local dispatch = SOURCE_METADATA_BY_SOURCE[source_type]
    if dispatch == nil or dispatch.mode == 'skip_with_info' then
        local reason
        if dispatch == nil then
            reason = 'no row-count SQL configured for source type ' .. tostring(source_type)
        else
            reason = dispatch.reason or 'metadata skipped'
        end
        return {
            available = false,
            rows = {},
            info_rows = { '-- PARALLEL_ROW_THRESHOLD gate skipped: ' .. reason },
        }
    end

    local pair_clauses = {}
    for _, p in ipairs(pair_list) do
        pair_clauses[#pair_clauses + 1] = string.format(dispatch.pair,
            escape_sql_literal(p.schema), escape_sql_literal(p.table_name))
    end
    local predicate = table.concat(pair_clauses, ' or ')
    local metadata_sql = (dispatch.template:gsub('<PREDICATE>', function() return predicate end))

    local outer_sql = "select * from (import into (src_schema varchar(2000), src_table varchar(2000), src_rows decimal(36,0), src_pk_col varchar(2000), src_pk_type varchar(200), src_date_col varchar(2000), src_num_col varchar(2000), src_partitioned boolean) from jdbc at "
        .. connection_name
        .. " statement '"
        .. escape_sql_literal(metadata_sql)
        .. "')"

    local success, lookup_res = pquery(outer_sql)
    if not success then
        local err = (lookup_res and lookup_res.error_message) or 'unknown error'
        return {
            available = false,
            rows = {},
            info_rows = { '-- PARALLEL_ROW_THRESHOLD gate skipped: row-count lookup failed (' .. tostring(err) .. ')' },
        }
    end

    local rows = {}
    for i = 1, #lookup_res do
        local r = lookup_res[i]
        local s = r.SRC_SCHEMA or r[1]
        local t = r.SRC_TABLE or r[2]
        if s ~= nil and t ~= nil then
            rows[tostring(s) .. '\t' .. tostring(t)] = {
                src_rows = r.SRC_ROWS or r[3],
                src_pk_col = r.SRC_PK_COL or r[4],
                src_pk_type = r.SRC_PK_TYPE or r[5],
                src_date_col = r.SRC_DATE_COL or r[6],
                src_num_col = r.SRC_NUM_COL or r[7],
                src_partitioned = r.SRC_PARTITIONED or r[8],
            }
        end
    end

    return { available = true, rows = rows, info_rows = {} }
end

-- Speq 1 gate: collapses multi-statement IMPORTs whose source row count is below
-- `PARALLEL_ROW_THRESHOLD` to a single statement. Consumes the shared metadata cache
-- populated by `transform_for_metadata`; never issues its own source-side query.
function transform_for_gate(res, options, cache)
    if res == nil or #res == 0 then return res end

    local threshold = parse_threshold(options)

    local out = {}
    for j = 1, #res do out[j] = res[j] end

    if cache ~= nil then
        for _, info_text in ipairs(cache.info_rows) do
            out[#out + 1] = { SQL_TEXT = info_text }
        end
    end

    if threshold <= 0 then
        return out
    end
    if cache == nil or not cache.available then
        return out
    end

    for i = 1, #res do
        local sql_text = first_sql_text(res[i])
        if classify_step(sql_text) == 'IMPORT' and count_statement_clauses(sql_text) > 1 then
            local src_schema, src_table = extract_source_ref_from_import(sql_text)
            if src_schema ~= nil and src_table ~= nil then
                local meta = cache.rows[src_schema .. '\t' .. src_table]
                local rowcount = meta and meta.src_rows
                local effective = tonumber(rowcount) or 0
                if effective < threshold then
                    out[i] = replace_row_sql(out[i], rewrite_to_first_statement(sql_text))
                end
            end
        end
    end
    return out
end

function parse_split_directive(options)
    local raw = blank_to_nil(opt(options, 'PARALLEL_SPLIT', 'AUTO')) or 'AUTO'
    local up = string.upper(raw)
    if up == 'AUTO' then return { mode = 'AUTO' } end
    if up == 'OFF' then return { mode = 'OFF' } end
    if up == 'PK' then return { mode = 'PK' } end
    if up == 'PARTITION' then return { mode = 'PARTITION' } end
    if up == 'ROWID' then return { mode = 'ROWID' } end
    if up == 'DATE' then return { mode = 'DATE' } end

    local prefix, rest = raw:match('^([^:]+):(.+)$')
    if prefix and string.upper(prefix) == 'DATE' then
        local col, grain = rest:match('^([^:]+):(.+)$')
        if col then return { mode = 'DATE', col = col, grain = string.upper(grain) } end
        return { mode = 'DATE', col = rest }
    end
    if prefix and string.upper(prefix) == 'HASH' then
        return { mode = 'HASH', col = rest }
    end
    error('Invalid PARALLEL_SPLIT value: ' .. tostring(raw))
end

-- Phase 2 N-resolver: explicit positive integer in PARALLEL_STATEMENTS yields N.
-- AUTO is a Phase-3 feature; it currently short-circuits to 1 (= no split). The
-- splitter is still wired so that Phase 3 only needs to swap this function out
-- for the row-count heuristic specified in parallel-auto-ceiling.
function resolve_split_n(options, src_rows)
    local raw = blank_to_nil(opt(options, 'PARALLEL_STATEMENTS', 'AUTO')) or 'AUTO'
    if string.upper(raw) == 'AUTO' then return 1 end
    local n = tonumber(raw)
    if n == nil then return 1 end
    n = math.floor(n)
    if n < 1 then return 1 end
    return n
end

function is_numeric_pk_type(type_str)
    if type_str == nil then return false end
    local up = string.upper(tostring(type_str))
    if up == '' then return false end
    if up:find('INT', 1, true) then return true end
    if up:find('NUMERIC', 1, true) then return true end
    if up:find('NUMBER', 1, true) then return true end
    if up:find('DECIMAL', 1, true) then return true end
    if up:find('FLOAT', 1, true) then return true end
    if up:find('DOUBLE', 1, true) then return true end
    if up:find('REAL', 1, true) then return true end
    if up == 'SERIAL' or up == 'BIGSERIAL' or up == 'SMALLSERIAL' then return true end
    return false
end

function pick_split_strategy(meta, options, dialect, source_type)
    local directive = parse_split_directive(options)
    if directive.mode == 'OFF' then return nil, nil end

    if directive.mode == 'AUTO' then
        if meta == nil then return nil, 'metadata cache empty' end
        if meta.src_pk_col and is_numeric_pk_type(meta.src_pk_type) then
            return { strategy = 'PK_RANGE', key = meta.src_pk_col }
        end
        if meta.src_date_col then
            return { strategy = 'DATE_BUCKET', key = meta.src_date_col }
        end
        if meta.src_num_col then
            return { strategy = 'HASH_NUM', key = meta.src_num_col }
        end
        if dialect and dialect.rowid_supported then
            return { strategy = 'ROWID', key = dialect.rowid_expr }
        end
        return nil, 'no usable split column for ' .. tostring(source_type)
    end

    if directive.mode == 'PK' then
        if meta and meta.src_pk_col and is_numeric_pk_type(meta.src_pk_type) then
            return { strategy = 'PK_RANGE', key = meta.src_pk_col }
        end
        return nil, 'PARALLEL_SPLIT=PK requested but no numeric PK in metadata'
    end

    if directive.mode == 'DATE' then
        local col = directive.col or (meta and meta.src_date_col)
        if col == nil then return nil, 'PARALLEL_SPLIT=DATE but no date column known' end
        return { strategy = 'DATE_BUCKET', key = col, grain = directive.grain }
    end

    if directive.mode == 'HASH' then
        if directive.col == nil then return nil, 'PARALLEL_SPLIT=HASH requires a column name' end
        return { strategy = 'HASH_NUM', key = directive.col }
    end

    if directive.mode == 'ROWID' then
        if dialect and dialect.rowid_supported then
            return { strategy = 'ROWID', key = dialect.rowid_expr }
        end
        return nil, 'PARALLEL_SPLIT=ROWID unsupported for ' .. tostring(source_type)
    end

    if directive.mode == 'PARTITION' then
        return nil, 'PARALLEL_SPLIT=PARTITION not supported in v1'
    end

    return nil, 'unsupported PARALLEL_SPLIT mode ' .. tostring(directive.mode)
end

function build_date_bucket(col, grain, dialect, n, k)
    if dialect == nil or dialect.month_fn == nil then return nil end

    local resolved_grain = grain
    if resolved_grain == nil then
        if n == 2 then resolved_grain = 'HALF'
        elseif n == 3 then resolved_grain = 'TRIMESTER'
        elseif n == 4 then resolved_grain = 'QUARTER'
        elseif n == 6 then resolved_grain = 'BIMONTH'
        elseif n == 12 then resolved_grain = 'MONTH'
        elseif n <= 31 then resolved_grain = 'DAY'
        else resolved_grain = 'YEAR_MONTH' end
    end

    local clause
    if resolved_grain == 'MONTH' then
        if n == 12 then
            clause = dialect.month_fn(col) .. ' = ' .. (k + 1)
        else
            local months = {}
            local m = k + 1
            while m <= 12 do months[#months + 1] = tostring(m); m = m + n end
            clause = dialect.month_fn(col) .. ' IN (' .. table.concat(months, ', ') .. ')'
        end
    elseif resolved_grain == 'QUARTER' then
        local m0 = k * 3 + 1
        clause = dialect.month_fn(col) .. ' IN (' .. m0 .. ', ' .. (m0 + 1) .. ', ' .. (m0 + 2) .. ')'
    elseif resolved_grain == 'HALF' then
        if k == 0 then clause = dialect.month_fn(col) .. ' IN (1, 2, 3, 4, 5, 6)'
        else clause = dialect.month_fn(col) .. ' IN (7, 8, 9, 10, 11, 12)' end
    elseif resolved_grain == 'TRIMESTER' then
        local m0 = k * 4 + 1
        clause = dialect.month_fn(col) .. ' IN (' .. m0 .. ', ' .. (m0 + 1) .. ', ' .. (m0 + 2) .. ', ' .. (m0 + 3) .. ')'
    elseif resolved_grain == 'BIMONTH' then
        local m0 = k * 2 + 1
        clause = dialect.month_fn(col) .. ' IN (' .. m0 .. ', ' .. (m0 + 1) .. ')'
    elseif resolved_grain == 'DAY' then
        if dialect.day_fn == nil then return nil end
        if k == n - 1 and n < 31 then
            local days = {}
            for d = n + 1, 31 do days[#days + 1] = tostring(d) end
            if #days == 0 then
                clause = dialect.day_fn(col) .. ' = ' .. (k + 1)
            else
                clause = dialect.day_fn(col) .. ' IN (' .. (k + 1) .. ', ' .. table.concat(days, ', ') .. ')'
            end
        else
            clause = dialect.day_fn(col) .. ' = ' .. (k + 1)
        end
    elseif resolved_grain == 'YEAR_MONTH' then
        if dialect.year_month_fn == nil then return nil end
        clause = 'MOD(' .. dialect.year_month_fn(col) .. ', ' .. n .. ') = ' .. k
    else
        return nil
    end

    if k == 0 then
        clause = '(' .. clause .. ' OR "' .. col .. '" IS NULL)'
    end
    return clause
end

function build_where_for_split(decision, dialect, n, k)
    if decision == nil then return nil end
    if decision.strategy == 'PK_RANGE' or decision.strategy == 'UNIQUE_NUM' then
        return 'MOD("' .. decision.key .. '", ' .. n .. ') = ' .. k
    end
    if decision.strategy == 'DATE_BUCKET' then
        return build_date_bucket(decision.key, decision.grain, dialect, n, k)
    end
    if decision.strategy == 'HASH_NUM' then
        if dialect == nil or dialect.hash_where == nil then return nil end
        return dialect.hash_where(decision.key, n, k)
    end
    if decision.strategy == 'ROWID' then
        if dialect == nil or dialect.rowid_where == nil then return nil end
        return dialect.rowid_where(n, k)
    end
    return nil
end

function append_where_to_inner_select(inner, where_clause)
    local lower = inner:lower()
    local where_pos = nil
    local cut_pos = #inner + 1

    local i = 1
    local in_string = false
    while i <= #inner do
        local c = inner:sub(i, i)
        if in_string then
            if c == "'" then
                if inner:sub(i + 1, i + 1) == "'" then i = i + 2
                else in_string = false; i = i + 1 end
            else i = i + 1 end
        elseif c == "'" then
            in_string = true; i = i + 1
        else
            local before_ok = (i == 1) or inner:sub(i - 1, i - 1):match('[%s%)]') ~= nil
            if before_ok then
                local six = lower:sub(i, i + 5)
                if (six == 'where ' or six == 'where\t' or six == 'where\n') and where_pos == nil then
                    where_pos = i
                end
                if six == 'group ' or six == 'order ' or six == 'having' then
                    if i < cut_pos then cut_pos = i end
                end
                if lower:sub(i, i + 4) == 'limit' then
                    local trail = inner:sub(i + 5, i + 5)
                    if trail == '' or trail:match('[%s]') then
                        if i < cut_pos then cut_pos = i end
                    end
                end
            end
            i = i + 1
        end
    end

    local head = inner:sub(1, cut_pos - 1)
    local tail = inner:sub(cut_pos)
    local trimmed = head:gsub('%s+$', '')

    if where_pos and where_pos < cut_pos then
        if tail == '' then return trimmed .. ' AND (' .. where_clause .. ')' end
        return trimmed .. ' AND (' .. where_clause .. ') ' .. tail
    end
    if tail == '' then return trimmed .. ' WHERE ' .. where_clause end
    return trimmed .. ' WHERE ' .. where_clause .. ' ' .. tail
end

function rewrite_import_to_multi_stmt(sql, where_per_k, n)
    local stmt_start = string.find(sql:lower(), "statement%s+'")
    if not stmt_start then return nil end
    local quote_open = sql:find("'", stmt_start)
    if not quote_open then return nil end

    local i = quote_open + 1
    local inner_end = nil
    while i <= #sql do
        local c = sql:sub(i, i)
        if c == "'" then
            if sql:sub(i + 1, i + 1) == "'" then i = i + 2
            else inner_end = i - 1; break end
        else i = i + 1 end
    end
    if inner_end == nil then return nil end

    local prefix = sql:sub(1, stmt_start - 1):gsub('%s+$', '')
    local inner_escaped = sql:sub(quote_open + 1, inner_end)
    local inner = inner_escaped:gsub("''", "'")

    local out = prefix
    for k = 0, n - 1 do
        local where = where_per_k[k + 1]
        if where == nil then return nil end
        local modified = append_where_to_inner_select(inner, where)
        local re_escaped = modified:gsub("'", "''")
        out = out .. " STATEMENT '" .. re_escaped .. "'"
    end
    return out
end

-- Speq 2 splitter: expands single-statement IMPORTs at-or-above
-- PARALLEL_ROW_THRESHOLD into N parallel STATEMENT clauses with pushdown-friendly
-- WHERE selectors picked via the split-strategy hierarchy. Multi-statement
-- IMPORTs are left untouched (the adapter already chose its split). Any failure
-- inside the splitter logs an INFO row and leaves the IMPORT unchanged - the
-- splitter is an optimization and MUST NOT break a migration.
function transform_for_split(res, options, cache, source_type)
    if res == nil or #res == 0 then return res end

    local directive_ok, directive = pcall(parse_split_directive, options)
    if not directive_ok then return res end
    if directive.mode == 'OFF' then return res end
    if cache == nil or not cache.available then return res end

    local threshold = parse_threshold(options)
    if threshold <= 0 then return res end

    local dialect = DIALECT_BY_SOURCE[source_type]

    local out = {}
    for j = 1, #res do out[j] = res[j] end

    local info_rows = {}

    for i = 1, #res do
        local sql_text = first_sql_text(res[i])
        if classify_step(sql_text) == 'IMPORT' and count_statement_clauses(sql_text) == 1 then
            local src_schema, src_table = extract_source_ref_from_import(sql_text)
            if src_schema ~= nil and src_table ~= nil then
                local meta = cache.rows[src_schema .. '\t' .. src_table]
                local rowcount = meta and tonumber(meta.src_rows) or 0
                if rowcount >= threshold then
                    local n = resolve_split_n(options, rowcount)
                    if n >= 2 then
                        local decision, reason = pick_split_strategy(meta, options, dialect, source_type)
                        if decision ~= nil then
                            local where_per_k = {}
                            local build_ok = true
                            for k = 0, n - 1 do
                                local w = build_where_for_split(decision, dialect, n, k)
                                if w == nil then build_ok = false; break end
                                where_per_k[#where_per_k + 1] = w
                            end
                            if build_ok then
                                local rewritten = rewrite_import_to_multi_stmt(sql_text, where_per_k, n)
                                if rewritten ~= nil then
                                    out[i] = replace_row_sql(out[i], rewritten)
                                else
                                    info_rows[#info_rows + 1] = '-- PARALLEL_SPLIT: rewrite failed for ' .. src_schema .. '.' .. src_table .. ' -- IMPORT left unchanged'
                                end
                            else
                                info_rows[#info_rows + 1] = '-- PARALLEL_SPLIT: WHERE-builder failed for ' .. src_schema .. '.' .. src_table .. ' -- IMPORT left unchanged'
                            end
                        elseif reason ~= nil then
                            info_rows[#info_rows + 1] = '-- PARALLEL_SPLIT: ' .. src_schema .. '.' .. src_table .. ' -> SINGLE (' .. reason .. ')'
                        end
                    end
                end
            end
        end
    end

    for _, t in ipairs(info_rows) do
        out[#out + 1] = { SQL_TEXT = t }
    end
    return out
end

function execute_adapter(adapter_sql, debug, ctx)
    local success, res = pquery(adapter_sql)
    if not success then
        error('"' .. res.error_message .. '" Caught while executing: "' .. res.statement_text .. '"')
    end

    if ctx ~= nil then
        local cache = transform_for_metadata(res, ctx.source_type, ctx.connection_name, ctx.options)
        res = transform_for_gate(res, ctx.options, cache)
        res = transform_for_split(res, ctx.options, cache, ctx.source_type)
    end

    if not debug then
        return execute_generated_sql(res), OUT_COLUMNS
    end

    return normalize_rows(res), OUT_COLUMNS
end

local source = normalize_source_type(SOURCE_TYPE)
local connection_name = require_value(CONNECTION_NAME, 'CONNECTION_NAME')
local connection_type = string.upper(blank_to_nil(CONNECTION_TYPE) or 'JDBC')
local db_filter = blank_to_nil(DB_FILTER) or '%'
local schema_filter = blank_to_nil(SCHEMA_FILTER) or '%'
local table_filter = blank_to_nil(TABLE_FILTER) or '%'
local target_schema = blank_to_nil(TARGET_SCHEMA)
local identifier_case_insensitive = parse_bool(IDENTIFIER_CASE_INSENSITIVE, true, 'IDENTIFIER_CASE_INSENSITIVE')
local debug = parse_bool(DEBUG, true, 'DEBUG')
local options = parse_options(OPTIONS)

local adapter_sql = nil

if source == 'MYSQL' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.MYSQL_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'MARIADB' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.MARIADB_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'POSTGRES' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.POSTGRES_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_string(target_schema) .. ')'

elseif source == 'REDSHIFT' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.REDSHIFT_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'DB2' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.DB2_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'VERTICA' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.VERTICA_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'HANA' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.HANA_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'AZURE_SQL' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.AZURE_SQL_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_bool(identifier_case_insensitive) .. ')'

elseif source == 'BIGQUERY' then
    local project_id = opt(options, 'PROJECT_ID', nil)
    if blank_to_nil(project_id) == nil and db_filter ~= '%' then
        project_id = db_filter
    end
    project_id = require_value(project_id, 'OPTIONS PROJECT_ID for BIGQUERY')

    adapter_sql = 'EXECUTE SCRIPT database_migration.BIGQUERY_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(project_id) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'DATABRICKS' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.DATABRICKS_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(opt_bool(options, 'CATALOG2SCHEMA', true)) .. ','
        .. sql_string(db_filter) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(target_schema) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_bool(identifier_case_insensitive) .. ')'

elseif source == 'SQLSERVER' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.SQLSERVER_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(opt_bool(options, 'DB2SCHEMA', false)) .. ','
        .. sql_string(db_filter) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(target_schema) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_bool(identifier_case_insensitive) .. ')'

elseif source == 'SNOWFLAKE' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.SNOWFLAKE_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(opt_bool(options, 'DB2SCHEMA', false)) .. ','
        .. sql_string(db_filter) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(target_schema) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_bool(identifier_case_insensitive) .. ')'

elseif source == 'ORACLE' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ','
        .. opt_sql_number(options, 'PARALLEL_STATEMENTS', 1) .. ','
        .. sql_bool(opt_bool(options, 'CREATE_PK', false)) .. ','
        .. sql_bool(opt_bool(options, 'CREATE_FK', false)) .. ','
        .. sql_bool(opt_bool(options, 'CHECK_MIGRATION', false)) .. ')'

elseif source == 'TERADATA' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.TERADATA_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_bool(opt_bool(options, 'CHECK_MIGRATION', false)) .. ')'

elseif source == 'EXASOL' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.EXASOL_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_string(connection_type) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_string(opt(options, 'GENERATE_VIEWS', 'FALSE')) .. ','
        .. sql_string(opt(options, 'VIEW_FILTER', '%')) .. ','
        .. sql_string(opt(options, 'PK_SETTING', 'DISABLE')) .. ')'

elseif source == 'NETEZZA' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.NETEZZA_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_string(db_filter) .. ','
        .. sql_string(schema_filter) .. ','
        .. sql_string(table_filter) .. ','
        .. sql_bool(identifier_case_insensitive) .. ')'

elseif source == 'VECTORWISE' then
    adapter_sql = 'EXECUTE SCRIPT database_migration.VECTORWISE_TO_EXASOL('
        .. sql_string(connection_name) .. ','
        .. sql_bool(identifier_case_insensitive) .. ','
        .. sql_string(table_filter) .. ')'

elseif source == 'S3' then
    error('S3 is not supported by MIGRATE_TO_EXASOL. Use DATABASE_MIGRATION.S3_PARALLEL_READ directly.')

else
    error('Unsupported SOURCE_TYPE: ' .. tostring(SOURCE_TYPE))
end

local ctx = {
    source_type = source,
    connection_name = connection_name,
    options = options,
}

return execute_adapter(adapter_sql, debug, ctx)
/

/*
Example:

execute script database_migration.MIGRATE_TO_EXASOL(
    'SNOWFLAKE',
    'SNOWFLAKE_CONNECTION',
    'JDBC',
    '%',
    '%',
    '%',
    NULL,
    TRUE,
    TRUE,
    'DB2SCHEMA=true'
);
*/
