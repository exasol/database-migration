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

ROW_COUNT_SQL_BY_SOURCE = {
    ORACLE = {
        mode = 'sql',
        template = "select owner, table_name, num_rows from all_tables where (<PREDICATE>)",
        pair = "(owner = '%s' and table_name = '%s')",
    },
    POSTGRES = {
        mode = 'sql',
        template = "select n.nspname, c.relname, c.reltuples::bigint from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r','p') and (<PREDICATE>)",
        pair = "(n.nspname = '%s' and c.relname = '%s')",
    },
    MYSQL = {
        mode = 'sql',
        template = "select table_schema, table_name, table_rows from information_schema.tables where (<PREDICATE>)",
        pair = "(table_schema = '%s' and table_name = '%s')",
    },
    SQLSERVER = {
        mode = 'sql',
        template = "select s.name as schema_name, t.name as table_name, sum(ps.row_count) as row_count from sys.tables t join sys.schemas s on s.schema_id = t.schema_id join sys.dm_db_partition_stats ps on ps.object_id = t.object_id and ps.index_id in (0,1) where (<PREDICATE>) group by s.name, t.name",
        pair = "(s.name = '%s' and t.name = '%s')",
    },
    SNOWFLAKE = {
        mode = 'sql',
        template = "select table_schema, table_name, row_count from information_schema.tables where (<PREDICATE>)",
        pair = "(table_schema = '%s' and table_name = '%s')",
    },
    BIGQUERY = {
        mode = 'skip_with_info',
        reason = 'per-dataset INFORMATION_SCHEMA lookup not yet implemented',
    },
    REDSHIFT = {
        mode = 'sql',
        template = [[select "schema", "table", tbl_rows from svv_table_info where (<PREDICATE>)]],
        pair = [[("schema" = '%s' and "table" = '%s')]],
    },
    VERTICA = {
        mode = 'sql',
        template = "select projection_schema, anchor_table_name, row_count from projection_storage where (<PREDICATE>)",
        pair = "(projection_schema = '%s' and anchor_table_name = '%s')",
    },
    DB2 = {
        mode = 'sql',
        template = "select tabschema, tabname, card from syscat.tables where (<PREDICATE>)",
        pair = "(tabschema = '%s' and tabname = '%s')",
    },
    HANA = {
        mode = 'sql',
        template = "select schema_name, table_name, record_count from sys.m_tables where (<PREDICATE>)",
        pair = "(schema_name = '%s' and table_name = '%s')",
    },
    NETEZZA = {
        mode = 'sql',
        template = [[select schema, tablename, reltuples from _v_table where (<PREDICATE>)]],
        pair = [[(schema = '%s' and tablename = '%s')]],
    },
    TERADATA = {
        mode = 'sql',
        template = "select databasename, tablename, currentpermspace from dbc.tablesizev where (<PREDICATE>)",
        pair = "(databasename = '%s' and tablename = '%s')",
    },
    DATABRICKS = {
        mode = 'sql',
        template = "select table_schema, table_name, cast(null as bigint) as row_count from information_schema.tables where (<PREDICATE>)",
        pair = "(table_schema = '%s' and table_name = '%s')",
    },
}
ROW_COUNT_SQL_BY_SOURCE.MARIADB = ROW_COUNT_SQL_BY_SOURCE.MYSQL
ROW_COUNT_SQL_BY_SOURCE.AZURE_SQL = ROW_COUNT_SQL_BY_SOURCE.SQLSERVER

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

function transform_for_gate(res, source_type, connection_name, options)
    if res == nil or #res == 0 then return res end

    local threshold_raw = opt(options, 'PARALLEL_ROW_THRESHOLD', '1000000')
    local threshold = tonumber(threshold_raw)
    if threshold == nil then
        error('Invalid numeric option PARALLEL_ROW_THRESHOLD: ' .. tostring(threshold_raw))
    end
    if threshold <= 0 then
        return res
    end

    local imports = {}
    local pair_list = {}
    local pairs_seen = {}
    local needs_lookup = false
    for i = 1, #res do
        local sql_text = first_sql_text(res[i])
        if classify_step(sql_text) == 'IMPORT' then
            local n = count_statement_clauses(sql_text)
            if n > 1 then
                local src_schema, src_table = extract_source_ref_from_import(sql_text)
                imports[#imports + 1] = {
                    idx = i,
                    sql = sql_text,
                    schema = src_schema,
                    table_name = src_table,
                }
                if src_schema ~= nil and src_table ~= nil then
                    local key = src_schema .. '\t' .. src_table
                    if not pairs_seen[key] then
                        pairs_seen[key] = true
                        pair_list[#pair_list + 1] = { schema = src_schema, table_name = src_table }
                    end
                    needs_lookup = true
                end
            end
        end
    end

    if not needs_lookup then
        return res
    end

    local function append_info(out, message)
        out[#out + 1] = { SQL_TEXT = '-- ' .. message }
        return out
    end

    local dispatch = ROW_COUNT_SQL_BY_SOURCE[source_type]
    if dispatch == nil or dispatch.mode == 'skip_with_info' then
        local reason
        if dispatch == nil then
            reason = 'no row-count SQL configured for source type ' .. tostring(source_type)
        else
            reason = dispatch.reason or 'gate skipped'
        end
        local out = {}
        for j = 1, #res do out[j] = res[j] end
        append_info(out, 'PARALLEL_ROW_THRESHOLD gate skipped: ' .. reason)
        return out
    end

    local pair_clauses = {}
    for _, p in ipairs(pair_list) do
        local esc_schema = escape_sql_literal(p.schema)
        local esc_table = escape_sql_literal(p.table_name)
        pair_clauses[#pair_clauses + 1] = string.format(dispatch.pair, esc_schema, esc_table)
    end
    local predicate = table.concat(pair_clauses, ' or ')
    local row_count_sql = (dispatch.template:gsub('<PREDICATE>', function() return predicate end))

    local outer_sql = "select * from (import into (src_schema varchar(2000), src_table varchar(2000), src_rows decimal(36,0)) from jdbc at "
        .. connection_name
        .. " statement '"
        .. escape_sql_literal(row_count_sql)
        .. "')"

    local success, lookup_res = pquery(outer_sql)
    if not success then
        local out = {}
        for j = 1, #res do out[j] = res[j] end
        local err = (lookup_res and lookup_res.error_message) or 'unknown error'
        append_info(out, 'PARALLEL_ROW_THRESHOLD gate skipped: row-count lookup failed (' .. tostring(err) .. ')')
        return out
    end

    local lookup = {}
    for i = 1, #lookup_res do
        local r = lookup_res[i]
        local s = r.SRC_SCHEMA or r[1]
        local t = r.SRC_TABLE or r[2]
        local n = r.SRC_ROWS or r[3]
        if s ~= nil and t ~= nil then
            lookup[tostring(s) .. '\t' .. tostring(t)] = n
        end
    end

    local out = {}
    for j = 1, #res do out[j] = res[j] end
    for _, im in ipairs(imports) do
        if im.schema ~= nil and im.table_name ~= nil then
            local rowcount = lookup[im.schema .. '\t' .. im.table_name]
            local effective = tonumber(rowcount) or 0
            if effective < threshold then
                out[im.idx] = replace_row_sql(out[im.idx], rewrite_to_first_statement(im.sql))
            end
        end
    end
    return out
end

function execute_adapter(adapter_sql, debug, gate_ctx)
    local success, res = pquery(adapter_sql)
    if not success then
        error('"' .. res.error_message .. '" Caught while executing: "' .. res.statement_text .. '"')
    end

    if gate_ctx ~= nil then
        res = transform_for_gate(res, gate_ctx.source_type, gate_ctx.connection_name, gate_ctx.options)
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

local gate_ctx = {
    source_type = source,
    connection_name = connection_name,
    options = options,
}

return execute_adapter(adapter_sql, debug, gate_ctx)
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
