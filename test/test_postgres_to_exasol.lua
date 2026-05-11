--[[
  Checks for postgres_to_exasol.sql.

  Run: lua test/test_postgres_to_exasol.lua
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
    local lua_block = content:match("%) RETURNS TABLE%s*\nAS\n(.-)\n/\n")
    assert(lua_block, "Could not extract Lua block from " .. path)
    return lua_block
end

local postgres_lua = extract_lua_block("postgres_to_exasol.sql")

local function run_postgres()
    local calls = {}
    local env = {
        CONNECTION_NAME = "POSTGRES_CONNECTION",
        IDENTIFIER_CASE_INSENSITIVE = true,
        SCHEMA_FILTER = "Public",
        TABLE_FILTER = "Order",
        DEST_SCHEMA = "DST",
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

    local fn, err = load(postgres_lua, "postgres_to_exasol.lua", "t", env)
    assert(fn, "Lua load failed: " .. tostring(err))

    local result_rows = fn()
    return {
        rows = result_rows,
        sql = calls[1],
        calls = calls,
    }
end

print("=== Lua Syntax Validation ===")

test("Lua block in postgres_to_exasol.sql has valid syntax", function()
    local fn, err = load(postgres_lua)
    assert(fn, "Lua syntax error in postgres_to_exasol.sql: " .. tostring(err))
end)

print("")
print("=== Generation Tests ===")

test("quotes Postgres source identifiers in generated import", function()
    local result = run_postgres()

    assert_contains(result.sql, "replace(\"column_name\", '\"', '\"\"')")
    assert_contains(result.sql, "replace(\"table_schema\", '\"', '\"\"')")
    assert_contains(result.sql, "replace(\"table_name\", '\"', '\"\"')")
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
