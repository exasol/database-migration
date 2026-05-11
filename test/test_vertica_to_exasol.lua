--[[
  Checks for vertica_to_exasol.sql.

  Run: lua test/test_vertica_to_exasol.lua
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

local vertica_lua = extract_lua_block("vertica_to_exasol.sql")

local function run_vertica(params)
    local calls = {}
    local rows = params.rows or {{SQL_TEXT = "-- ### SCHEMAS ###"}}

    local env = {
        CONNECTION_NAME = params.connection_name or "VERTICA_CONNECTION",
        IDENTIFIER_CASE_INSENSITIVE = params.identifier_case_insensitive,
        SCHEMA_FILTER = params.schema_filter or "%",
        TABLE_FILTER = params.table_filter or "%",
        string = string,
        table = table,
        tostring = tostring,
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

    local fn, err = load(vertica_lua, "vertica_to_exasol.lua", "t", env)
    assert(fn, "Lua load failed: " .. tostring(err))

    local result_rows = fn()
    return {
        rows = result_rows,
        sql = calls[1],
        calls = calls,
    }
end

print("=== Lua Syntax Validation ===")

test("Lua block in vertica_to_exasol.sql has valid syntax", function()
    local fn, err = load(vertica_lua)
    assert(fn, "Lua syntax error in vertica_to_exasol.sql: " .. tostring(err))
end)

print("")
print("=== Generation Tests ===")

test("maps Vertica TIMESTAMP to Exasol TIMESTAMP", function()
    local result = run_vertica({})

    assert_contains(result.sql, "when upper(data_type) = 'TIMESTAMP' then 'TIMESTAMP'")
    assert_not_contains(result.sql, "when upper(data_type) = 'TIMESTAMP' then 'varchar(14)'")
end)

test("quotes Vertica source identifiers in generated import", function()
    local result = run_vertica({})

    assert_contains(result.sql, "replace(table_schema, '\"', '\"\"')")
    assert_contains(result.sql, "replace(table_name, '\"', '\"\"')")
    assert_contains(result.sql, "replace(column_name, '\"', '\"\"')")
    assert_contains(result.sql, "source_column_name")
    assert_contains(result.sql, "source_table_schema")
    assert_contains(result.sql, "source_table_name")
end)

test("matches parameterized Vertica binary type names", function()
    local result = run_vertica({})

    assert_contains(result.sql, "when upper(data_type) LIKE 'BINARY%' then 'char('||(coalesce(character_maximum_length, data_type_length) * 2)||')'")
    assert_contains(result.sql, "when upper(data_type) LIKE 'VARBINARY%' then 'varchar('||(coalesce(character_maximum_length, data_type_length) * 2)||')'")
    assert_contains(result.sql, "when upper(data_type) LIKE 'BINARY%' then 'TO_HEX('||\"source_column_name\"||')'")
    assert_contains(result.sql, "when upper(data_type) LIKE 'VARBINARY%' then 'TO_HEX('||\"source_column_name\"||')'")
end)

test("matches parameterized Vertica numeric type names", function()
    local result = run_vertica({})

    assert_contains(result.sql, "when upper(data_type) LIKE 'DECIMAL%' or upper(data_type) LIKE 'NUMERIC%' or upper(data_type) LIKE 'NUMBER%'")
    assert_not_contains(result.sql, "when upper(data_type) in ('DECIMAL','NUMERIC','NUMBER')")
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
