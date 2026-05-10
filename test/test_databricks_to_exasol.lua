--[[
  Checks for databricks_to_exasol.sql.

  Run: lua test/test_databricks_to_exasol.lua
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

local function assert_not_contains(actual, pattern, msg)
    if tostring(actual):find(pattern, 1, true) then
        error((msg or "unexpected text") .. ": " .. pattern .. " in " .. tostring(actual))
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

local databricks_lua = extract_lua_block("databricks_to_exasol.sql")

local function run_databricks(params)
    local calls = {}
    local rows = params.rows or {{SQL_TEXT = "-- ### SCHEMAS ###"}}

    local env = {
        CONNECTION_NAME = params.connection_name or "DATABRICKS_CONNECTION",
        CATALOG2SCHEMA = params.catalog2schema,
        CATALOG_FILTER = params.catalog_filter,
        SCHEMA_FILTER = params.schema_filter,
        TARGET_SCHEMA = params.target_schema,
        TABLE_FILTER = params.table_filter,
        IDENTIFIER_CASE_INSENSITIVE = params.identifier_case_insensitive,
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
        if params.metadata_error then
            return false, {
                error_message = params.metadata_error,
                statement_text = sql,
            }
        end
        return true, rows
    end

    local fn, err = load(databricks_lua, "databricks_to_exasol.lua", "t", env)
    assert(fn, "Lua load failed: " .. tostring(err))

    local result_rows = fn()
    return {
        rows = result_rows,
        sql = calls[1],
        calls = calls,
    }
end

print("=== Lua Syntax Validation ===")

test("Lua block in databricks_to_exasol.sql has valid syntax", function()
    local fn, err = load(databricks_lua)
    assert(fn, "Lua syntax error in databricks_to_exasol.sql: " .. tostring(err))
end)

print("")
print("=== Generation Tests ===")

test("generates migration sql for Databricks tables", function()
    local result = run_databricks({})

    assert_eq(#result.calls, 1)
    assert_eq(result.rows[1].SQL_TEXT, "-- ### SCHEMAS ###")
    assert_contains(result.sql, "IMPORT FROM JDBC DRIVER = 'DATABRICKS' AT DATABRICKS_CONNECTION")
    assert_contains(result.sql, "INFORMATION_SCHEMA.COLUMNS")
    assert_contains(result.sql, "INFORMATION_SCHEMA.TABLES")
    assert_contains(result.sql, "table_type in (''MANAGED'', ''EXTERNAL'', ''MANAGED_SHALLOW_CLONE'', ''EXTERNAL_SHALLOW_CLONE'')")
    assert_contains(result.sql, "-- ### SCHEMAS ###")
    assert_contains(result.sql, "-- ### TABLES ###")
    assert_contains(result.sql, "-- ### IMPORTS ###")
    assert_contains(result.sql, "import into")
end)

test("applies source filters to metadata query", function()
    local result = run_databricks({
        catalog_filter = "main, analytics",
        schema_filter = "bronze%",
        table_filter = "orders, customers",
    })

    assert_contains(result.sql, "table_catalog in (''main'',''analytics'')")
    assert_contains(result.sql, "table_schema like ''bronze%''")
    assert_contains(result.sql, "table_name in (''orders'',''customers'')")
end)

test("catalog2schema maps catalog schema table safely", function()
    local result = run_databricks({catalog2schema = true})

    assert_contains(result.sql, "\"exa_table_catalog\" as \"target_schema_name\"")
    assert_contains(result.sql, "\"exa_table_schema\" || '_' || \"exa_table_name\" as \"target_table_name\"")
end)

test("target schema override is honored", function()
    local result = run_databricks({
        catalog2schema = true,
        target_schema = "MART",
    })

    assert_contains(result.sql, "'MART' as \"target_schema_name\"")
    assert_contains(result.sql, "\"exa_table_catalog\" || '_' || \"exa_table_schema\" || '_' || \"exa_table_name\" as \"target_table_name\"")
end)

test("identifier case option controls generated names", function()
    local upper_result = run_databricks({identifier_case_insensitive = true})
    local preserve_result = run_databricks({identifier_case_insensitive = false})

    assert_contains(upper_result.sql, "upper(databricks.\"table_catalog\")")
    assert_contains(upper_result.sql, "upper(databricks.\"column_name\")")
    assert_not_contains(preserve_result.sql, "upper(databricks.\"table_catalog\")")
    assert_not_contains(preserve_result.sql, "upper(databricks.\"column_name\")")
end)

test("maps databricks types to exasol types", function()
    local result = run_databricks({})

    assert_contains(result.sql, "when upper(data_type) = 'TINYINT'")
    assert_contains(result.sql, "DECIMAL(3,0)")
    assert_contains(result.sql, "when upper(data_type) = 'BIGINT'")
    assert_contains(result.sql, "DECIMAL(19,0)")
    assert_contains(result.sql, "when upper(data_type) = 'DECIMAL'")
    assert_contains(result.sql, "when upper(data_type) = 'STRING'")
    assert_contains(result.sql, "VARCHAR(2000000)")
    assert_contains(result.sql, "when upper(data_type) = 'TIMESTAMP_NTZ'")
    assert_contains(result.sql, "when upper(data_type) = 'ARRAY'")
    assert_contains(result.sql, "when upper(data_type) = 'OBJECT'")
    assert_contains(result.sql, "--LOSSY_DATATYPE")
end)

test("metadata errors include source statement", function()
    local ok, err = pcall(run_databricks, {metadata_error = "permission denied"})

    assert_eq(ok, false)
    assert_contains(err, "permission denied")
    assert_contains(err, "IMPORT FROM JDBC DRIVER = 'DATABRICKS' AT DATABRICKS_CONNECTION")
end)

test("databricks adapter does not generate s3 loader sql", function()
    local script = read_file("databricks_to_exasol.sql")
    local result = run_databricks({})

    assert_not_contains(script, "S3_PARALLEL_READ")
    assert_not_contains(result.sql, "S3_PARALLEL_READ")
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
