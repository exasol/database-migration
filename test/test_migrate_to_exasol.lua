--[[
  Checks for migrate_to_exasol.sql.

  Run: lua test/test_migrate_to_exasol.lua
]]

local passed = 0
local failed = 0

local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then
        passed = passed + 1
        print("  PASS: " .. name)
    else
        failed = failed + 1
        print("  FAIL: " .. name .. " -- " .. tostring(err))
    end
end

local function assert_eq(actual, expected, msg)
    if actual ~= expected then
        error((msg or "") .. " expected: " .. tostring(expected) .. ", got: " .. tostring(actual))
    end
end

local function assert_contains(actual, pattern, msg)
    if not tostring(actual):find(pattern, 1, true) then
        error((msg or "missing expected text") .. ": " .. pattern .. " in " .. tostring(actual))
    end
end

local function read_file(path)
    local f = io.open(path, "r")
    assert(f, "Could not open " .. path)
    local content = f:read("*a")
    f:close()
    return content
end

local function extract_lua_block(path)
    local content = read_file(path)
    local lua_block = content:match("%) RETURNS TABLE%s*\nAS\n(.-)\n/\n")
    assert(lua_block, "Could not extract Lua block from " .. path)
    return lua_block
end

local migrate_lua = extract_lua_block("migrate_to_exasol.sql")

local function run_migrate(params)
    local calls = {}
    local adapter_rows = params.adapter_rows or {{SQL_TEXT = "select 1"}}
    local execute_results = params.execute_results or {}

    local env = {
        SOURCE_TYPE = params.source_type,
        CONNECTION_NAME = params.connection_name or "SRC_CONN",
        CONNECTION_TYPE = params.connection_type,
        DB_FILTER = params.db_filter,
        SCHEMA_FILTER = params.schema_filter,
        TABLE_FILTER = params.table_filter,
        TARGET_SCHEMA = params.target_schema,
        IDENTIFIER_CASE_INSENSITIVE = params.identifier_case_insensitive,
        DEBUG = params.debug,
        OPTIONS = params.options,
        string = string,
        table = table,
        math = math,
        tostring = tostring,
        tonumber = tonumber,
        type = type,
        error = error,
    }

    env.pquery = function(sql)
        calls[#calls + 1] = sql
        if #calls == 1 then
            if params.adapter_error then
                return false, {
                    error_message = params.adapter_error,
                    statement_text = sql,
                }
            end
            return true, adapter_rows
        end

        local result = execute_results[#calls - 1]
        if result and result.success == false then
            return false, {error_message = result.error_message or "execution failed"}
        end
        return true, {rows_affected = 0}
    end

    local fn, err = load(migrate_lua, "migrate_to_exasol.lua", "t", env)
    assert(fn, "Lua load failed: " .. tostring(err))

    local rows, columns = fn()
    return {
        rows = rows,
        columns = columns,
        calls = calls,
        adapter_sql = calls[1],
    }
end

local function default_case(source_type, expected_sql)
    test("dispatches " .. source_type, function()
        local result = run_migrate({
            source_type = source_type,
            connection_type = "JDBC",
            db_filter = "DB",
            schema_filter = "SCH",
            table_filter = "TBL",
            target_schema = "DST",
            identifier_case_insensitive = true,
            debug = true,
            options = "",
        })
        assert_eq(result.adapter_sql, expected_sql)
        assert_eq(result.rows[1][1], "select 1")
        assert_eq(result.rows[1][2], "PREVIEW")
        assert_eq(result.columns, "SQL_TEXT VARCHAR(2000000), SUCCESS VARCHAR(10), ERROR_MESSAGE VARCHAR(20000)")
    end)
end

print("=== Lua Syntax Validation ===")

test("Lua block in migrate_to_exasol.sql has valid syntax", function()
    local fn, err = load(migrate_lua)
    assert(fn, "Lua syntax error in migrate_to_exasol.sql: " .. tostring(err))
end)

print("")
print("=== MIGRATE_TO_EXASOL Dispatch Tests ===")

default_case("MYSQL", "EXECUTE SCRIPT database_migration.MYSQL_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL')")
default_case("MARIADB", "EXECUTE SCRIPT database_migration.MARIADB_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL')")
default_case("POSTGRES", "EXECUTE SCRIPT database_migration.POSTGRES_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL','DST')")
default_case("REDSHIFT", "EXECUTE SCRIPT database_migration.REDSHIFT_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL')")
default_case("DB2", "EXECUTE SCRIPT database_migration.DB2_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL')")
default_case("VERTICA", "EXECUTE SCRIPT database_migration.VERTICA_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL')")
default_case("HANA", "EXECUTE SCRIPT database_migration.HANA_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL')")
default_case("AZURE_SQL", "EXECUTE SCRIPT database_migration.AZURE_SQL_TO_EXASOL('SRC_CONN','SCH','TBL',TRUE)")
default_case("BIGQUERY", "EXECUTE SCRIPT database_migration.BIGQUERY_TO_EXASOL('SRC_CONN',TRUE,'DB','SCH','TBL')")
default_case("DATABRICKS", "EXECUTE SCRIPT database_migration.DATABRICKS_TO_EXASOL('SRC_CONN',TRUE,'DB','SCH','DST','TBL',TRUE)")
default_case("SQLSERVER", "EXECUTE SCRIPT database_migration.SQLSERVER_TO_EXASOL('SRC_CONN',FALSE,'DB','SCH','DST','TBL',TRUE)")
default_case("SNOWFLAKE", "EXECUTE SCRIPT database_migration.SNOWFLAKE_TO_EXASOL('SRC_CONN',FALSE,'DB','SCH','DST','TBL',TRUE)")
default_case("ORACLE", "EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL',1,FALSE,FALSE,FALSE)")
default_case("TERADATA", "EXECUTE SCRIPT database_migration.TERADATA_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL',FALSE)")
default_case("EXASOL", "EXECUTE SCRIPT database_migration.EXASOL_TO_EXASOL('SRC_CONN','JDBC',TRUE,'SCH','TBL','FALSE','%','DISABLE')")
default_case("NETEZZA", "EXECUTE SCRIPT database_migration.NETEZZA_TO_EXASOL('SRC_CONN','DB','SCH','TBL',TRUE)")
default_case("VECTORWISE", "EXECUTE SCRIPT database_migration.VECTORWISE_TO_EXASOL('SRC_CONN',TRUE,'TBL')")

print("")
print("=== Alias And Option Tests ===")

test("source aliases normalize before dispatch", function()
    local result = run_migrate({
        source_type = "sql server",
        db_filter = "DB",
        schema_filter = "SCH",
        table_filter = "TBL",
        target_schema = "DST",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.SQLSERVER_TO_EXASOL('SRC_CONN',FALSE,'DB','SCH','DST','TBL',TRUE)")
end)

test("Databricks aliases normalize before dispatch", function()
    local result = run_migrate({
        source_type = "databricks sql",
        db_filter = "CAT",
        schema_filter = "SCH",
        table_filter = "TBL",
        target_schema = "DST",
        options = "CATALOG2SCHEMA=false",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.DATABRICKS_TO_EXASOL('SRC_CONN',FALSE,'CAT','SCH','DST','TBL',TRUE)")
end)

test("string literals are escaped in generated adapter calls", function()
    local result = run_migrate({
        source_type = "MYSQL",
        connection_name = "CONN'X",
        schema_filter = "S'CH",
        table_filter = "T'BL",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.MYSQL_TO_EXASOL('CONN''X',TRUE,'S''CH','T''BL')")
end)

test("Snowflake DB2SCHEMA option is forwarded", function()
    local result = run_migrate({
        source_type = "SNOWFLAKE",
        db_filter = "DB",
        schema_filter = "SCH",
        table_filter = "TBL",
        target_schema = "DST",
        options = "DB2SCHEMA=true",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.SNOWFLAKE_TO_EXASOL('SRC_CONN',TRUE,'DB','SCH','DST','TBL',TRUE)")
end)

test("Oracle options are forwarded", function()
    local result = run_migrate({
        source_type = "ORACLE",
        schema_filter = "SCH",
        table_filter = "TBL",
        options = "PARALLEL_STATEMENTS=4;CREATE_PK=yes;CREATE_FK=1;CHECK_MIGRATION=true",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('SRC_CONN',TRUE,'SCH','TBL',4,TRUE,TRUE,TRUE)")
end)

test("Exasol options and connection type are forwarded", function()
    local result = run_migrate({
        source_type = "EXASOL",
        connection_type = "exa",
        schema_filter = "SCH",
        table_filter = "TBL",
        options = "GENERATE_VIEWS=TRUE;VIEW_FILTER=VW%;PK_SETTING=ENABLE",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.EXASOL_TO_EXASOL('SRC_CONN','EXA',TRUE,'SCH','TBL','TRUE','VW%','ENABLE')")
end)

test("BigQuery PROJECT_ID option overrides DB_FILTER", function()
    local result = run_migrate({
        source_type = "BIGQUERY",
        db_filter = "DB",
        schema_filter = "SCH",
        table_filter = "TBL",
        options = "PROJECT_ID=PROJECT",
    })
    assert_eq(result.adapter_sql, "EXECUTE SCRIPT database_migration.BIGQUERY_TO_EXASOL('SRC_CONN',TRUE,'PROJECT','SCH','TBL')")
end)

print("")
print("=== Execution Mode Tests ===")

test("DEBUG false runs generated statements for non-native adapters", function()
    local result = run_migrate({
        source_type = "MYSQL",
        schema_filter = "SCH",
        table_filter = "TBL",
        debug = false,
        adapter_rows = {
            {SQL_TEXT = "-- comment"},
            {SQL_TEXT = "create table t(c int)"},
        },
    })

    assert_eq(#result.calls, 2)
    assert_eq(result.calls[2], "create table t(c int)")
    assert_eq(result.rows[1][1], "-- The following statements were executed successfully.")
    assert_eq(result.rows[2][2], "SKIPPED")
    assert_eq(result.rows[3][2], "TRUE")
end)

test("DEBUG false runs Snowflake generated statements", function()
    local result = run_migrate({
        source_type = "SNOWFLAKE",
        debug = false,
        adapter_rows = {
            {SQL_TEXT = "create table t(c int)"},
        },
    })

    assert_eq(#result.calls, 2)
    assert_eq(result.calls[2], "create table t(c int)")
    assert_eq(result.rows[1][1], "-- The following statements were executed successfully.")
    assert_eq(result.rows[2][2], "TRUE")
end)

test("DEBUG false reports no executable generated statements", function()
    local result = run_migrate({
        source_type = "MYSQL",
        debug = false,
        adapter_rows = {
            {SQL_TEXT = "-- ### SCHEMAS ###"},
            {SQL_TEXT = "-- ### TABLES ###"},
        },
    })

    assert_eq(#result.calls, 1)
    assert_eq(result.rows[1][1], "-- No executable SQL statements were generated.")
    assert_eq(result.rows[2][2], "SKIPPED")
    assert_eq(result.rows[3][2], "SKIPPED")
end)

test("DEBUG false reports empty adapter output", function()
    local result = run_migrate({
        source_type = "MYSQL",
        debug = false,
        adapter_rows = {},
    })

    assert_eq(#result.calls, 1)
    assert_eq(#result.rows, 1)
    assert_eq(result.rows[1][1], "-- No executable SQL statements were generated.")
    assert_eq(result.rows[1][2], "SKIPPED")
end)

print("")
print("=== Error Handling Tests ===")

test("unsupported source raises a clear error", function()
    local ok, err = pcall(run_migrate, {source_type = "UNKNOWN"})
    assert_eq(ok, false)
    assert_contains(err, "Unsupported SOURCE_TYPE")
end)

test("S3 points users to the direct loader", function()
    local ok, err = pcall(run_migrate, {source_type = "S3"})
    assert_eq(ok, false)
    assert_contains(err, "S3 is not supported by MIGRATE_TO_EXASOL")
    assert_contains(err, "S3_PARALLEL_READ")
end)

test("missing connection raises a clear error", function()
    local ok, err = pcall(run_migrate, {source_type = "MYSQL", connection_name = ""})
    assert_eq(ok, false)
    assert_contains(err, "CONNECTION_NAME is required")
end)

test("BigQuery requires PROJECT_ID when DB_FILTER is wildcard", function()
    local ok, err = pcall(run_migrate, {
        source_type = "BIGQUERY",
        db_filter = "%",
        schema_filter = "SCH",
        table_filter = "TBL",
    })
    assert_eq(ok, false)
    assert_contains(err, "OPTIONS PROJECT_ID for BIGQUERY is required")
end)

test("invalid boolean option raises a clear error", function()
    local ok, err = pcall(run_migrate, {
        source_type = "SNOWFLAKE",
        options = "DB2SCHEMA=maybe",
    })
    assert_eq(ok, false)
    assert_contains(err, "Invalid boolean for DB2SCHEMA")
end)

test("invalid DEBUG raises a clear error", function()
    local ok, err = pcall(run_migrate, {
        source_type = "MYSQL",
        debug = "maybe",
    })
    assert_eq(ok, false)
    assert_contains(err, "Invalid boolean for DEBUG")
end)

test("adapter errors include source statement", function()
    local ok, err = pcall(run_migrate, {
        source_type = "MYSQL",
        adapter_error = "adapter failed",
    })
    assert_eq(ok, false)
    assert_contains(err, "adapter failed")
    assert_contains(err, "MYSQL_TO_EXASOL")
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
