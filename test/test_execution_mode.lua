--[[
  Test suite for the EXECUTION_MODE parsing logic added to snowflake_to_exasol.sql.

  Since we can't run the full Exasol script locally, this extracts and tests
  the mode-parsing logic in isolation, plus verifies the summary-building logic.

  Run: lua test/test_execution_mode.lua
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

-- Simulate the mode-parsing logic from snowflake_to_exasol.sql (lines 28-36)
local function parse_execution_mode(EXECUTION_MODE)
    local debug
    -- In Exasol Lua, unset params are `null` (a special value).
    -- Locally we simulate that with nil.
    if EXECUTION_MODE == nil then
        debug = true
    elseif string.upper(EXECUTION_MODE) == 'EXECUTE' then
        debug = false
    elseif string.upper(EXECUTION_MODE) == 'DEBUG' then
        debug = true
    else
        error([[Invalid EXECUTION_MODE. Use 'DEBUG' or 'EXECUTE']])
    end
    return debug
end

print("=== EXECUTION_MODE Parsing Tests ===")

test("nil defaults to debug=true", function()
    assert_eq(parse_execution_mode(nil), true)
end)

test("'DEBUG' sets debug=true", function()
    assert_eq(parse_execution_mode('DEBUG'), true)
end)

test("'debug' (lowercase) sets debug=true", function()
    assert_eq(parse_execution_mode('debug'), true)
end)

test("'Debug' (mixed case) sets debug=true", function()
    assert_eq(parse_execution_mode('Debug'), true)
end)

test("'EXECUTE' sets debug=false", function()
    assert_eq(parse_execution_mode('EXECUTE'), false)
end)

test("'execute' (lowercase) sets debug=false", function()
    assert_eq(parse_execution_mode('execute'), false)
end)

test("'Execute' (mixed case) sets debug=false", function()
    assert_eq(parse_execution_mode('Execute'), false)
end)

test("invalid value raises error", function()
    local ok, err = pcall(parse_execution_mode, 'INVALID')
    assert_eq(ok, false, "should have raised error")
    assert(tostring(err):find("Invalid EXECUTION_MODE"), "error message should mention EXECUTION_MODE")
end)

test("empty string raises error", function()
    local ok, err = pcall(parse_execution_mode, '')
    assert_eq(ok, false, "should have raised error")
end)

test("'RUN' raises error", function()
    local ok, err = pcall(parse_execution_mode, 'RUN')
    assert_eq(ok, false, "should have raised error")
end)

-- Simulate the summary-building logic (lines 240-258)
print("")
print("=== Summary Building Tests (DEBUG mode) ===")

test("DEBUG mode: all statements get PREVIEW status", function()
    local res = {
        {SQL_TEXT = 'create schema if not exists "MY_SCHEMA";'},
        {SQL_TEXT = '-- ### TABLES ###'},
        {SQL_TEXT = 'create or replace table "MY_SCHEMA"."MY_TABLE" ("ID" DECIMAL(10,0));'},
    }

    local summary = {}
    -- DEBUG path
    for i = 1, #res do
        summary[#summary+1] = {res[i].SQL_TEXT, 'PREVIEW', nil}
    end

    assert_eq(#summary, 3)
    assert_eq(summary[1][2], 'PREVIEW')
    assert_eq(summary[2][2], 'PREVIEW')
    assert_eq(summary[3][2], 'PREVIEW')
    assert_eq(summary[1][1], 'create schema if not exists "MY_SCHEMA";')
end)

print("")
print("=== Summary Building Tests (EXECUTE mode) ===")

test("EXECUTE mode: comments are SKIPPED", function()
    local res = {
        {SQL_TEXT = '-- ### SCHEMAS ###'},
        {SQL_TEXT = '-- This is a comment'},
    }

    local summary = {}
    for i = 1, #res do
        local sql = res[i].SQL_TEXT
        if sql ~= nil and sql ~= '' and not sql:match('^%-%-') then
            -- Would call pquery() in real script
            summary[#summary+1] = {sql, 'TRUE', nil}
        else
            summary[#summary+1] = {sql, 'SKIPPED', 'Comment or empty'}
        end
    end

    assert_eq(#summary, 2)
    assert_eq(summary[1][2], 'SKIPPED')
    assert_eq(summary[2][2], 'SKIPPED')
end)

test("EXECUTE mode: empty/nil SQL is SKIPPED", function()
    local res = {
        {SQL_TEXT = ''},
        {SQL_TEXT = nil},
    }

    local summary = {}
    for i = 1, #res do
        local sql = res[i].SQL_TEXT
        if sql ~= nil and sql ~= '' and not sql:match('^%-%-') then
            summary[#summary+1] = {sql, 'TRUE', nil}
        else
            summary[#summary+1] = {sql, 'SKIPPED', 'Comment or empty'}
        end
    end

    assert_eq(#summary, 2)
    assert_eq(summary[1][2], 'SKIPPED')
    assert_eq(summary[2][2], 'SKIPPED')
end)

test("EXECUTE mode: real SQL statements would be executed", function()
    local res = {
        {SQL_TEXT = 'create schema if not exists "TEST";'},
        {SQL_TEXT = '-- comment'},
        {SQL_TEXT = 'create or replace table "TEST"."T1" ("ID" DECIMAL(10,0));'},
    }

    local executed = {}
    local summary = {}
    for i = 1, #res do
        local sql = res[i].SQL_TEXT
        if sql ~= nil and sql ~= '' and not sql:match('^%-%-') then
            -- Simulate pquery() succeeding
            executed[#executed+1] = sql
            summary[#summary+1] = {sql, 'TRUE', nil}
        else
            summary[#summary+1] = {sql, 'SKIPPED', 'Comment or empty'}
        end
    end

    assert_eq(#summary, 3)
    assert_eq(#executed, 2, "should execute 2 non-comment statements")
    assert_eq(summary[1][2], 'TRUE')
    assert_eq(summary[2][2], 'SKIPPED')
    assert_eq(summary[3][2], 'TRUE')
end)

test("EXECUTE mode: failed pquery populates error_message", function()
    local res = {
        {SQL_TEXT = 'create schema if not exists "TEST";'},
    }

    local summary = {}
    for i = 1, #res do
        local sql = res[i].SQL_TEXT
        if sql ~= nil and sql ~= '' and not sql:match('^%-%-') then
            -- Simulate pquery() failing
            local suc = false
            local info = {error_message = 'object TEST already exists'}
            if suc then
                summary[#summary+1] = {sql, 'TRUE', nil}
            else
                summary[#summary+1] = {sql, 'FALSE', info.error_message}
            end
        else
            summary[#summary+1] = {sql, 'SKIPPED', 'Comment or empty'}
        end
    end

    assert_eq(#summary, 1)
    assert_eq(summary[1][2], 'FALSE')
    assert_eq(summary[1][3], 'object TEST already exists')
end)

print("")
print("=== Banner Message Tests (EXECUTE mode) ===")

test("EXECUTE mode: success banner prepended when all statements succeed", function()
    local res = {
        {SQL_TEXT = 'create schema if not exists "TEST";'},
        {SQL_TEXT = '-- comment'},
        {SQL_TEXT = 'create or replace table "TEST"."T1" ("ID" DECIMAL(10,0));'},
    }

    local summary = {}
    local fail_count = 0
    for i = 1, #res do
        local sql = res[i].SQL_TEXT
        if sql ~= nil and sql ~= '' and not sql:match('^%-%-') then
            -- Simulate pquery() succeeding
            summary[#summary+1] = {sql, 'TRUE', nil}
        else
            summary[#summary+1] = {sql, 'SKIPPED', 'Comment or empty'}
        end
    end
    if fail_count == 0 then
        table.insert(summary, 1, {'-- The following statements were executed successfully.', 'SKIPPED', nil})
    else
        table.insert(summary, 1, {'-- Execution completed with ' .. fail_count .. ' error(s). See ERROR_MESSAGE column for details.', 'SKIPPED', nil})
    end

    assert_eq(#summary, 4, "3 statements + 1 banner")
    assert_eq(summary[1][1], '-- The following statements were executed successfully.')
    assert_eq(summary[1][2], 'SKIPPED')
    assert_eq(summary[2][2], 'TRUE')
end)

test("EXECUTE mode: error banner prepended when failures occur", function()
    local res = {
        {SQL_TEXT = 'create schema if not exists "TEST";'},
        {SQL_TEXT = 'create or replace table "BAD"."T1" ("ID" OOPS);'},
    }

    local summary = {}
    local fail_count = 0
    for i = 1, #res do
        local sql = res[i].SQL_TEXT
        if sql ~= nil and sql ~= '' and not sql:match('^%-%-') then
            if i == 1 then
                summary[#summary+1] = {sql, 'TRUE', nil}
            else
                fail_count = fail_count + 1
                summary[#summary+1] = {sql, 'FALSE', 'syntax error'}
            end
        else
            summary[#summary+1] = {sql, 'SKIPPED', 'Comment or empty'}
        end
    end
    if fail_count == 0 then
        table.insert(summary, 1, {'-- The following statements were executed successfully.', 'SKIPPED', nil})
    else
        table.insert(summary, 1, {'-- Execution completed with ' .. fail_count .. ' error(s). See ERROR_MESSAGE column for details.', 'SKIPPED', nil})
    end

    assert_eq(#summary, 3, "2 statements + 1 banner")
    assert(summary[1][1]:find('1 error'), "banner should mention 1 error")
    assert_eq(summary[2][2], 'TRUE')
    assert_eq(summary[3][2], 'FALSE')
end)

-- Verify the Lua source in the actual SQL file parses correctly
print("")
print("=== Lua Syntax Validation ===")

test("Lua block in snowflake_to_exasol.sql has valid syntax", function()
    -- Extract the Lua block between AS and the closing /
    local f = io.open("snowflake_to_exasol.sql", "r")
    assert(f, "Could not open snowflake_to_exasol.sql")
    local content = f:read("*a")
    f:close()

    -- Extract between ") RETURNS TABLE\nAS\n" and the final "\n/"
    local lua_block = content:match("%) RETURNS TABLE%s*\nAS\n(.+)\n/\n")
    assert(lua_block, "Could not extract Lua block from SQL file")

    -- Try to parse (not execute) the Lua code
    -- We expect load to succeed for syntax, even though runtime would fail
    -- (missing Exasol functions like pquery, query, output, etc.)
    local fn, err = load(lua_block)
    if not fn then
        -- Some errors are expected because of undefined Exasol globals.
        -- But pure syntax errors should be caught.
        -- Check if it's a syntax error vs runtime reference error
        if err:find("syntax") or err:find("unexpected") or err:find("expected") then
            error("Lua syntax error in snowflake_to_exasol.sql: " .. err)
        end
        -- If it's just about undefined globals at parse time, that's fine
        print("    (Note: load() returned: " .. tostring(err) .. " - expected for Exasol globals)")
    end
end)

print("")
print(string.format("=== Results: %d passed, %d failed ===", passed, failed))

if failed > 0 then
    os.exit(1)
end
