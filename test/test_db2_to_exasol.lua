--[[
  Checks for db2_to_exasol.sql.

  Run: lua test/test_db2_to_exasol.lua
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

local function read_file(path)
    local f = io.open(path, "r")
    assert(f, "Could not open " .. path)
    local content = f:read("*a")
    f:close()
    return content
end

local function extract_lua_block(path)
    local content = read_file(path)
    local lua_block = content:match("%)%s*RETURNS TABLE%s*\nAS\n(.-)\n/\n")
    assert(lua_block, "Could not extract Lua block from " .. path)
    return lua_block
end

local db2_lua = extract_lua_block("db2_to_exasol.sql")

local function run_db2()
    local calls = {}
    local env = {
        CONNECTION_NAME = "DB2_CONNECTION",
        IDENTIFIER_CASE_INSENSITIVE = true,
        SCHEMA_FILTER = "DB2DT",
        TABLE_FILTER = "CORE_CASE",
        string = string,
        table = table,
        tostring = tostring,
        type = type,
        error = error,
    }

    env.query = function(sql)
        calls[#calls + 1] = sql
        return {{SQL_TEXT = "-- ### SCHEMAS ###"}}
    end

    local fn, err = load(db2_lua, "db2_to_exasol.lua", "t", env)
    assert(fn, "Lua load failed: " .. tostring(err))

    local result_rows = fn()
    return {
        rows = result_rows,
        sql = calls[1],
        calls = calls,
    }
end

print("=== Lua Syntax Validation ===")

test("Lua block in db2_to_exasol.sql has valid syntax", function()
    local fn, err = load(db2_lua)
    assert(fn, "Lua syntax error in db2_to_exasol.sql: " .. tostring(err))
end)

print("")
print("=== Generation Tests ===")

test("trims DB2 catalog schema/table filters case-insensitively", function()
    local result = run_db2()

    assert_contains(result.sql, "upper(rtrim(t.table_schema)) like upper(''DB2DT'')")
    assert_contains(result.sql, "upper(rtrim(t.table_name)) like upper(''CORE_CASE'')")
    assert_contains(result.sql, "rtrim(t.table_schema) not in")
end)

test("quotes DB2 source identifiers in generated import", function()
    local result = run_db2()

    assert_contains(result.sql, [['"' || replace(table_schema, '"', '""') || '"' as "source_table_schema"]])
    assert_contains(result.sql, [['"' || replace(table_name, '"', '""') || '"' as "source_table_name"]])
    assert_contains(result.sql, [['"' || replace(column_name, '"', '""') || '"' as "source_column_name"]])
    assert_contains(result.sql, "then \"source_column_name\"")
    assert_contains(result.sql, "' from ' || \"source_table_schema\"|| '.' || \"source_table_name\"")
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
