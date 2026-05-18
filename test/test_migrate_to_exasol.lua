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
        os = os,
        tostring = tostring,
        tonumber = tonumber,
        type = type,
        error = error,
        ipairs = ipairs,
        pairs = pairs,
        pcall = pcall,
        select = select,
    }

    local execute_call_index = 0
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

        if sql:find("import into (src_schema", 1, true) then
            if params.gate_lookup_error then
                return false, {error_message = params.gate_lookup_error}
            end
            return true, params.gate_lookup_rows or {}
        end

        execute_call_index = execute_call_index + 1
        local result = execute_results[execute_call_index]
        if result and result.success == false then
            return false, {error_message = result.error_message or "execution failed"}
        end
        local affected = (result and result.rows_affected) or 0
        return true, {rows_affected = affected}
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

local AUDIT_COLUMNS = "STEP_KIND VARCHAR(40), TARGET_OBJ VARCHAR(2000), ROWS_AFFECTED DECIMAL(18,0), ELAPSED_MS DECIMAL(18,0), RESULT_FLAG VARCHAR(20), SQL_TEXT VARCHAR(2000000), ERROR_MESSAGE VARCHAR(20000)"

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
        assert_eq(result.rows[1][1], "OTHER")
        assert_eq(result.rows[1][5], "PREVIEW")
        assert_eq(result.rows[1][6], "select 1")
        assert_eq(result.rows[2][1], "SUMMARY")
        assert_eq(result.rows[2][5], "PREVIEW")
        assert_eq(result.columns, AUDIT_COLUMNS)
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
    assert_eq(result.rows[1][1], "INFO")
    assert_eq(result.rows[1][5], "SKIPPED")
    assert_eq(result.rows[2][1], "CREATE_TABLE")
    assert_eq(result.rows[2][5], "OK")
    assert_eq(result.rows[3][1], "SUMMARY")
    assert_eq(result.rows[3][5], "OK")
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
    assert_eq(result.rows[1][1], "CREATE_TABLE")
    assert_eq(result.rows[1][5], "OK")
    assert_eq(result.rows[2][1], "SUMMARY")
    assert_eq(result.rows[2][5], "OK")
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
    assert_eq(result.rows[1][1], "INFO")
    assert_eq(result.rows[1][5], "SKIPPED")
    assert_eq(result.rows[2][1], "INFO")
    assert_eq(result.rows[2][5], "SKIPPED")
    assert_eq(result.rows[3][1], "SUMMARY")
    assert_eq(result.rows[3][2], "No executable SQL generated")
    assert_eq(result.rows[3][5], "SKIPPED")
end)

test("DEBUG false reports empty adapter output", function()
    local result = run_migrate({
        source_type = "MYSQL",
        debug = false,
        adapter_rows = {},
    })

    assert_eq(#result.calls, 1)
    assert_eq(#result.rows, 1)
    assert_eq(result.rows[1][1], "SUMMARY")
    assert_eq(result.rows[1][2], "No executable SQL generated")
    assert_eq(result.rows[1][5], "SKIPPED")
end)

test("STEP_KIND classifies CREATE_SCHEMA / CREATE_TABLE / IMPORT", function()
    local result = run_migrate({
        source_type = "MYSQL",
        debug = false,
        adapter_rows = {
            {SQL_TEXT = 'create schema if not exists "MART"'},
            {SQL_TEXT = 'create or replace table "MART"."ORDERS" ("ID" DECIMAL(18,0))'},
            {SQL_TEXT = 'import into "MART"."ORDERS" from jdbc at SRC_CONN statement \'select id from orders\''},
        },
    })

    assert_eq(result.rows[1][1], "CREATE_SCHEMA")
    assert_eq(result.rows[1][2], "MART")
    assert_eq(result.rows[2][1], "CREATE_TABLE")
    assert_eq(result.rows[2][2], "MART.ORDERS")
    assert_eq(result.rows[3][1], "IMPORT")
    assert_eq(result.rows[3][2], "MART.ORDERS")
    assert_eq(result.rows[4][1], "SUMMARY")
end)

test("ROWS_AFFECTED captured for IMPORT in execute mode", function()
    local result = run_migrate({
        source_type = "MYSQL",
        debug = false,
        adapter_rows = {
            {SQL_TEXT = 'import into "M"."T" from jdbc at SRC statement \'select 1\''},
        },
        execute_results = {[1] = {success = true, rows_affected = 42}},
    })

    assert_eq(result.rows[1][1], "IMPORT")
    assert_eq(result.rows[1][3], 42)
    assert_eq(result.rows[2][1], "SUMMARY")
    assert_eq(result.rows[2][3], 42)
end)

test("Errors mark RESULT_FLAG ERROR and SUMMARY ERROR", function()
    local result = run_migrate({
        source_type = "MYSQL",
        debug = false,
        adapter_rows = {
            {SQL_TEXT = 'create table "T" ("c" INT)'},
        },
        execute_results = {[1] = {success = false, error_message = "boom"}},
    })

    assert_eq(result.rows[1][5], "ERROR")
    assert_eq(result.rows[1][7], "boom")
    assert_eq(result.rows[2][1], "SUMMARY")
    assert_eq(result.rows[2][5], "ERROR")
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
print("=== PARALLEL_ROW_THRESHOLD Gate Tests ===")

local function multi_stmt_import(target_schema, target_table, src_schema, src_table, n)
    local s = 'IMPORT INTO "' .. target_schema .. '"."' .. target_table
        .. '" ("ID") FROM JDBC AT SRC_CONN'
    for i = 1, n do
        s = s .. " STATEMENT 'select \"ID\" from \"" .. src_schema .. '"."' .. src_table
            .. "\" where mod(\"ID\"," .. n .. ")=" .. (i - 1) .. "'"
    end
    return s
end

local function single_stmt_import(target_schema, target_table, src_schema, src_table)
    return 'IMPORT INTO "' .. target_schema .. '"."' .. target_table
        .. '" ("ID") FROM JDBC AT SRC_CONN STATEMENT \'select "ID" from "'
        .. src_schema .. '"."' .. src_table .. '"\''
end

local function find_import_row(rows, target)
    for i = 1, #rows do
        if rows[i][1] == "IMPORT" and rows[i][2] == target then
            return rows[i]
        end
    end
    return nil
end

test("gate rewrites multi-statement IMPORT below threshold", function()
    local sql = multi_stmt_import("DST", "SMALL_T", "SMOKE", "SMALL_T", 4)
    local result = run_migrate({
        source_type = "ORACLE",
        schema_filter = "SMOKE",
        table_filter = "SMALL_T",
        options = "PARALLEL_STATEMENTS=4;PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "SMALL_T", SRC_ROWS = 500000}},
    })
    assert_eq(#result.calls, 2)
    local row = find_import_row(result.rows, "DST.SMALL_T")
    assert(row, "IMPORT row missing")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 1, "expected exactly one STATEMENT clause after rewrite, got " .. count)
    assert_contains(row[6], '"DST"."SMALL_T"')
    assert_contains(row[6], '"SMOKE"."SMALL_T"')
end)

test("gate keeps multi-statement IMPORT at or above threshold", function()
    local sql = multi_stmt_import("DST", "BIG_T", "SMOKE", "BIG_T", 4)
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_STATEMENTS=4;PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "BIG_T", SRC_ROWS = 5000000}},
    })
    local row = find_import_row(result.rows, "DST.BIG_T")
    assert(row, "IMPORT row missing")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 4, "expected all four STATEMENT clauses preserved, got " .. count)
end)

test("gate + splitter off leaves single-statement IMPORT untouched", function()
    local sql = single_stmt_import("DST", "MED_T", "SMOKE", "MED_T")
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=1;PARALLEL_SPLIT=OFF",
        adapter_rows = {{SQL_TEXT = sql}},
    })
    assert_eq(#result.calls, 1, "no metadata lookup should fire when splitter and gate are both inactive on single-stmt IMPORTs")
    local row = find_import_row(result.rows, "DST.MED_T")
    assert(row, "IMPORT row missing")
    assert_eq(row[6], sql)
end)

test("gate treats NULL num_rows as below threshold", function()
    local sql = multi_stmt_import("DST", "NO_STATS", "SMOKE", "NO_STATS", 4)
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "NO_STATS", SRC_ROWS = nil}},
    })
    local row = find_import_row(result.rows, "DST.NO_STATS")
    assert(row, "IMPORT row missing")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 1, "NULL row count should be treated as below threshold; got " .. count)
end)

test("gate disabled by PARALLEL_ROW_THRESHOLD=0", function()
    local sql_multi = multi_stmt_import("DST", "SMALL_T", "SMOKE", "SMALL_T", 4)
    local sql_single = single_stmt_import("DST", "MED_T", "SMOKE", "MED_T")
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=0",
        adapter_rows = {{SQL_TEXT = sql_multi}, {SQL_TEXT = sql_single}},
    })
    assert_eq(#result.calls, 1, "PARALLEL_ROW_THRESHOLD=0 must issue no row-count lookup")
    local row = find_import_row(result.rows, "DST.SMALL_T")
    assert(row, "multi-stmt IMPORT row missing")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 4, "all clauses preserved under threshold=0")
end)

test("gate default threshold is 1000000", function()
    local sql = multi_stmt_import("DST", "T", "SMOKE", "T", 4)
    local result = run_migrate({
        source_type = "ORACLE",
        options = "",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "T", SRC_ROWS = 999999}},
    })
    local row = find_import_row(result.rows, "DST.T")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 1, "default threshold 1000000 should rewrite 999999-row table; got " .. count)
end)

test("gate soft-fails on row-count lookup error", function()
    local sql = multi_stmt_import("DST", "T", "SMOKE", "T", 4)
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_error = "ORA-00942: table or view does not exist",
    })
    local row = find_import_row(result.rows, "DST.T")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 4, "soft-fail must leave all statement clauses intact")
    local info_found = false
    for i = 1, #result.rows do
        if result.rows[i][1] == "INFO" and tostring(result.rows[i][6]):find("gate skipped", 1, true) then
            info_found = true
            break
        end
    end
    assert(info_found, "expected INFO row noting gate skip on lookup error")
end)

test("gate skips lookup when adapter emits no IMPORTs", function()
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {
            {SQL_TEXT = 'create schema if not exists "DST"'},
            {SQL_TEXT = 'create or replace table "DST"."T" ("ID" DECIMAL(18,0))'},
        },
    })
    assert_eq(#result.calls, 1, "no IMPORTs -> no lookup")
end)

test("gate skips unsupported source type with INFO row", function()
    local sql = multi_stmt_import("DST", "T", "SMOKE", "T", 4)
    local result = run_migrate({
        source_type = "EXASOL",
        options = "PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {{SQL_TEXT = sql}},
    })
    assert_eq(#result.calls, 1, "unsupported source must not issue lookup")
    local row = find_import_row(result.rows, "DST.T")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 4, "IMPORT preserved when gate not configured")
    local info_found = false
    for i = 1, #result.rows do
        if result.rows[i][1] == "INFO" and tostring(result.rows[i][6]):find("no row-count SQL configured", 1, true) then
            info_found = true
            break
        end
    end
    assert(info_found, "expected INFO row noting unconfigured source")
end)

test("gate keys lookup on source schema parsed from inner SELECT", function()
    local sql = 'IMPORT INTO "DST"."ORDERS" ("ID") FROM JDBC AT SRC_CONN'
        .. " STATEMENT 'select \"ID\" from \"PUBLIC\".\"orders\" where mod(\"ID\",2)=0'"
        .. " STATEMENT 'select \"ID\" from \"PUBLIC\".\"orders\" where mod(\"ID\",2)=1'"
    local lookup_sql_seen = nil
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "PUBLIC", SRC_TABLE = "orders", SRC_ROWS = 100}},
    })
    for i = 1, #result.calls do
        if tostring(result.calls[i]):find("import into (src_schema", 1, true) then
            lookup_sql_seen = result.calls[i]
        end
    end
    assert(lookup_sql_seen, "row-count lookup SQL not seen")
    assert_contains(lookup_sql_seen, "PUBLIC")
    assert_contains(lookup_sql_seen, "orders")
    assert(not lookup_sql_seen:find("\"DST\"", 1, true), "lookup must NOT key on target schema DST")
    local row = find_import_row(result.rows, "DST.ORDERS")
    local _, count = string.gsub(row[6], "STATEMENT '", "STATEMENT '")
    assert_eq(count, 1, "below-threshold IMPORT must be rewritten to single statement")
end)

test("gate applies per-table within one migration with one lookup", function()
    local sql_small = multi_stmt_import("DST", "SMALL_T", "SMOKE", "SMALL_T", 4)
    local sql_big = multi_stmt_import("DST", "BIG_T", "SMOKE", "BIG_T", 4)
    local sql_med = single_stmt_import("DST", "MED_T", "SMOKE", "MED_T")
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=1000000",
        adapter_rows = {
            {SQL_TEXT = sql_small},
            {SQL_TEXT = sql_big},
            {SQL_TEXT = sql_med},
        },
        gate_lookup_rows = {
            {SRC_SCHEMA = "SMOKE", SRC_TABLE = "SMALL_T", SRC_ROWS = 500},
            {SRC_SCHEMA = "SMOKE", SRC_TABLE = "BIG_T", SRC_ROWS = 5000000},
        },
    })
    local lookup_count = 0
    for i = 1, #result.calls do
        if tostring(result.calls[i]):find("import into (src_schema", 1, true) then
            lookup_count = lookup_count + 1
        end
    end
    assert_eq(lookup_count, 1, "exactly one row-count lookup per migration")
    local small = find_import_row(result.rows, "DST.SMALL_T")
    local big = find_import_row(result.rows, "DST.BIG_T")
    local med = find_import_row(result.rows, "DST.MED_T")
    local _, small_count = string.gsub(small[6], "STATEMENT '", "STATEMENT '")
    local _, big_count = string.gsub(big[6], "STATEMENT '", "STATEMENT '")
    local _, med_count = string.gsub(med[6], "STATEMENT '", "STATEMENT '")
    assert_eq(small_count, 1, "SMALL_T should be rewritten to 1 statement")
    assert_eq(big_count, 4, "BIG_T should retain 4 statements")
    assert_eq(med_count, 1, "MED_T single-stmt unchanged")
end)

print("")
print("=== PARALLEL_SPLIT Dispatcher Tests ===")

local function count_clauses(sql)
    local _, n = string.gsub(sql, "STATEMENT '", "STATEMENT '")
    return n
end

test("splitter expands single-statement IMPORT on numeric PK (PK_RANGE)", function()
    local sql = single_stmt_import("DST", "ORDERS", "PUBLIC", "orders")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "PUBLIC", SRC_TABLE = "orders", SRC_ROWS = 20000000, SRC_PK_COL = "ORDER_ID", SRC_PK_TYPE = "int8"}},
    })
    local row = find_import_row(result.rows, "DST.ORDERS")
    assert(row, "IMPORT row missing")
    assert_eq(count_clauses(row[6]), 4, "expected 4 STATEMENT clauses; got " .. count_clauses(row[6]))
    assert_contains(row[6], 'MOD("ORDER_ID", 4) = 0')
    assert_contains(row[6], 'MOD("ORDER_ID", 4) = 3')
    assert_contains(row[6], '"PUBLIC"."orders"')
    assert_contains(row[6], '"DST"."ORDERS"')
end)

test("splitter expands single-statement IMPORT on date column (DATE_BUCKET, quarter)", function()
    local sql = single_stmt_import("DST", "EVENTS", "SMOKE", "EVENTS")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "EVENTS", SRC_ROWS = 20000000, SRC_DATE_COL = "EVENT_DT"}},
    })
    local row = find_import_row(result.rows, "DST.EVENTS")
    assert(row, "IMPORT row missing")
    assert_eq(count_clauses(row[6]), 4)
    assert_contains(row[6], 'EXTRACT(MONTH FROM "EVENT_DT") IN (1, 2, 3)')
    assert_contains(row[6], 'EXTRACT(MONTH FROM "EVENT_DT") IN (10, 11, 12)')
    assert_contains(row[6], '"EVENT_DT" IS NULL')
end)

test("splitter falls through to HASH_NUM when no PK + no date col", function()
    local sql = single_stmt_import("DST", "SESSIONS", "SMOKE", "SESSIONS")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "SESSIONS", SRC_ROWS = 20000000, SRC_NUM_COL = "CUSTOMER_ID"}},
    })
    local row = find_import_row(result.rows, "DST.SESSIONS")
    assert_eq(count_clauses(row[6]), 4)
    assert_contains(row[6], 'HASHTEXT("CUSTOMER_ID"')
end)

test("splitter picks ROWID when no PK/date/num and source supports it", function()
    local sql = single_stmt_import("DST", "HEAP_T", "SMOKE", "HEAP_T")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "HEAP_T", SRC_ROWS = 20000000}},
    })
    local row = find_import_row(result.rows, "DST.HEAP_T")
    assert_eq(count_clauses(row[6]), 4)
    assert_contains(row[6], 'HASHTEXT(ctid::text)')
end)

test("splitter falls back to SINGLE + INFO row when no usable column (no ROWID support)", function()
    local sql = single_stmt_import("DST", "MYSTERY_T", "SMOKE", "MYSTERY_T")
    local result = run_migrate({
        source_type = "SNOWFLAKE",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "MYSTERY_T", SRC_ROWS = 20000000}},
    })
    local row = find_import_row(result.rows, "DST.MYSTERY_T")
    assert_eq(count_clauses(row[6]), 1, "IMPORT must remain single-statement when no split column")
    assert_eq(row[6], sql)
    local info_found = false
    for i = 1, #result.rows do
        if result.rows[i][1] == "INFO" and tostring(result.rows[i][6]):find("SMOKE.MYSTERY_T", 1, true) then
            info_found = true
            break
        end
    end
    assert(info_found, "expected INFO row noting splitter SINGLE fallback")
end)

test("splitter ignores single-stmt IMPORT below threshold", function()
    local sql = single_stmt_import("DST", "SMALL_T", "SMOKE", "SMALL_T")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "SMALL_T", SRC_ROWS = 500000, SRC_PK_COL = "ID", SRC_PK_TYPE = "int8"}},
    })
    local row = find_import_row(result.rows, "DST.SMALL_T")
    assert_eq(count_clauses(row[6]), 1, "below-threshold IMPORT must stay single-statement")
    assert_eq(row[6], sql)
end)

test("splitter leaves multi-statement IMPORTs unchanged (pass-through)", function()
    local sql = multi_stmt_import("DST", "BIG_T", "SMOKE", "BIG_T", 4)
    local result = run_migrate({
        source_type = "ORACLE",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "BIG_T", SRC_ROWS = 20000000, SRC_PK_COL = "ID", SRC_PK_TYPE = "NUMBER"}},
    })
    local row = find_import_row(result.rows, "DST.BIG_T")
    assert_eq(count_clauses(row[6]), 4, "multi-stmt IMPORT must retain its 4 STATEMENT clauses")
end)

test("PARALLEL_SPLIT=OFF disables splitter despite usable metadata", function()
    local sql = single_stmt_import("DST", "ORDERS", "PUBLIC", "orders")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=OFF",
        adapter_rows = {{SQL_TEXT = sql}},
    })
    assert_eq(#result.calls, 1, "no metadata lookup should fire when PARALLEL_SPLIT=OFF and no multi-stmt IMPORTs")
    local row = find_import_row(result.rows, "DST.ORDERS")
    assert_eq(row[6], sql)
end)

test("PARALLEL_SPLIT=DATE:col:grain forces named column and grain", function()
    local sql = single_stmt_import("DST", "EVENTS", "SMOKE", "EVENTS")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=DATE:CREATED_AT:QUARTER",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "EVENTS", SRC_ROWS = 20000000, SRC_PK_COL = "EVENT_ID", SRC_PK_TYPE = "int8", SRC_DATE_COL = "EVENT_DT"}},
    })
    local row = find_import_row(result.rows, "DST.EVENTS")
    assert_eq(count_clauses(row[6]), 4)
    assert_contains(row[6], 'EXTRACT(MONTH FROM "CREATED_AT")')
    assert(not row[6]:find('EVENT_DT'), "named override must not consult cache.src_date_col")
    assert(not row[6]:find('EVENT_ID'), "named override must not consult cache.src_pk_col")
end)

test("PARALLEL_SPLIT=HASH:col forces named column", function()
    local sql = single_stmt_import("DST", "SESSIONS", "SMOKE", "SESSIONS")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=HASH:CUSTOMER_ID",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "SMOKE", SRC_TABLE = "SESSIONS", SRC_ROWS = 20000000, SRC_PK_COL = "SESSION_ID", SRC_PK_TYPE = "int8"}},
    })
    local row = find_import_row(result.rows, "DST.SESSIONS")
    assert_eq(count_clauses(row[6]), 4)
    assert_contains(row[6], 'HASHTEXT("CUSTOMER_ID"')
end)

test("splitter preserves IMPORT target + column list during rewrite", function()
    local sql = 'IMPORT INTO "DST"."ORDERS" ("A","B","C") FROM JDBC AT SRC_CONN STATEMENT \'select "A","B","C" from "PUBLIC"."orders"\''
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=2;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql}},
        gate_lookup_rows = {{SRC_SCHEMA = "PUBLIC", SRC_TABLE = "orders", SRC_ROWS = 5000000, SRC_PK_COL = "A", SRC_PK_TYPE = "int8"}},
    })
    local row = find_import_row(result.rows, "DST.ORDERS")
    assert_contains(row[6], 'IMPORT INTO "DST"."ORDERS" ("A","B","C")')
    assert_contains(row[6], 'select "A","B","C" from "PUBLIC"."orders"')
    assert_eq(count_clauses(row[6]), 2)
end)

test("metadata round-trip fires once for mixed single/multi-stmt IMPORTs", function()
    local sql_multi = multi_stmt_import("DST", "BIG_T", "SMOKE", "BIG_T", 4)
    local sql_single = single_stmt_import("DST", "ORDERS", "PUBLIC", "orders")
    local result = run_migrate({
        source_type = "POSTGRES",
        target_schema = "DST",
        options = "PARALLEL_ROW_THRESHOLD=1000000;PARALLEL_STATEMENTS=4;PARALLEL_SPLIT=AUTO",
        adapter_rows = {{SQL_TEXT = sql_multi}, {SQL_TEXT = sql_single}},
        gate_lookup_rows = {
            {SRC_SCHEMA = "SMOKE", SRC_TABLE = "BIG_T", SRC_ROWS = 500},
            {SRC_SCHEMA = "PUBLIC", SRC_TABLE = "orders", SRC_ROWS = 20000000, SRC_PK_COL = "ID", SRC_PK_TYPE = "int8"},
        },
    })
    local lookup_count = 0
    for i = 1, #result.calls do
        if tostring(result.calls[i]):find("import into (src_schema", 1, true) then
            lookup_count = lookup_count + 1
        end
    end
    assert_eq(lookup_count, 1, "exactly one metadata round-trip across gate + splitter")
    local multi_row = find_import_row(result.rows, "DST.BIG_T")
    local single_row = find_import_row(result.rows, "DST.ORDERS")
    assert_eq(count_clauses(multi_row[6]), 1, "below-threshold multi-stmt collapsed by gate")
    assert_eq(count_clauses(single_row[6]), 4, "above-threshold single-stmt expanded by splitter")
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
