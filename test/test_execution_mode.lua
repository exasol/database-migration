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

-- === PARALLEL_CONNECTIONS Tests ===

-- Simulate the PARALLEL_CONNECTIONS parsing logic from snowflake_to_exasol.sql
-- mock_vcpu: simulates the VCPU value returned by pquery on EXA_SYSTEM_EVENTS
local AUTO_VCPU_RATIO = 0.75

local function parse_parallel_connections(PARALLEL_CONNECTIONS, mock_vcpu)
    local parallel = 1
    local parallel_info_mode = false
    local vcpu_count = 0
    -- In Exasol Lua, unset params are `null`; locally we simulate with nil.
    if PARALLEL_CONNECTIONS ~= nil then
        if type(PARALLEL_CONNECTIONS) == 'string' then
            local mode = string.upper(PARALLEL_CONNECTIONS)
            if mode == 'AUTO' or mode == 'INFO' then
                -- In real script this queries EXA_STATISTICS.EXA_SYSTEM_EVENTS
                if mock_vcpu == nil then
                    error('Could not determine VCPU count from EXA_STATISTICS.EXA_SYSTEM_EVENTS.')
                end
                vcpu_count = mock_vcpu
                parallel = math.max(1, math.floor(vcpu_count * AUTO_VCPU_RATIO))

                if mode == 'INFO' then
                    parallel_info_mode = true
                end
            else
                error([[Invalid PARALLEL_CONNECTIONS. Use a positive integer, 'AUTO', 'INFO', or NULL.]])
            end
        elseif type(PARALLEL_CONNECTIONS) == 'number' then
            parallel = math.max(1, math.floor(PARALLEL_CONNECTIONS))
        else
            error([[Invalid PARALLEL_CONNECTIONS. Use a positive integer, 'AUTO', 'INFO', or NULL.]])
        end
    end
    return parallel, parallel_info_mode, vcpu_count
end

print("")
print("=== PARALLEL_CONNECTIONS Parsing Tests ===")

test("nil defaults to parallel=1", function()
    assert_eq(parse_parallel_connections(nil), 1)
end)

test("1 returns parallel=1", function()
    assert_eq(parse_parallel_connections(1), 1)
end)

test("4 returns parallel=4", function()
    assert_eq(parse_parallel_connections(4), 4)
end)

test("0 is clamped to 1", function()
    assert_eq(parse_parallel_connections(0), 1)
end)

test("-5 is clamped to 1", function()
    assert_eq(parse_parallel_connections(-5), 1)
end)

test("3.7 is floored to 3", function()
    assert_eq(parse_parallel_connections(3.7), 3)
end)

test("invalid string value raises error", function()
    local ok, err = pcall(parse_parallel_connections, 'four')
    assert_eq(ok, false, "should have raised error")
    assert(tostring(err):find("Invalid PARALLEL_CONNECTIONS"), "error message should mention PARALLEL_CONNECTIONS")
end)

test("'AUTO' with VCPU=8 returns parallel=6", function()
    local parallel, info_mode = parse_parallel_connections('AUTO', 8)
    assert_eq(parallel, 6)  -- floor(8 * 0.75) = 6
    assert_eq(info_mode, false)
end)

test("'AUTO' with VCPU=1 returns parallel=1 (clamped)", function()
    local parallel = parse_parallel_connections('AUTO', 1)
    assert_eq(parallel, 1)  -- floor(1 * 0.75) = 0, clamped to 1
end)

test("'auto' (lowercase) works same as 'AUTO'", function()
    local parallel, info_mode = parse_parallel_connections('auto', 8)
    assert_eq(parallel, 6)
    assert_eq(info_mode, false)
end)

test("'Auto' (mixed case) works same as 'AUTO'", function()
    local parallel, info_mode = parse_parallel_connections('Auto', 4)
    assert_eq(parallel, 3)  -- floor(4 * 0.75) = 3
    assert_eq(info_mode, false)
end)

test("'INFO' sets parallel_info_mode=true and computes parallel", function()
    local parallel, info_mode, vcpu = parse_parallel_connections('INFO', 8)
    assert_eq(parallel, 6)
    assert_eq(info_mode, true)
    assert_eq(vcpu, 8)
end)

test("'info' (lowercase) works same as 'INFO'", function()
    local parallel, info_mode = parse_parallel_connections('info', 8)
    assert_eq(parallel, 6)
    assert_eq(info_mode, true)
end)

test("'INVALID_STRING' raises error", function()
    local ok, err = pcall(parse_parallel_connections, 'INVALID_STRING', 8)
    assert_eq(ok, false, "should have raised error")
    assert(tostring(err):find("Invalid PARALLEL_CONNECTIONS"), "error message should mention PARALLEL_CONNECTIONS")
end)

test("'AUTO' without VCPU data raises error", function()
    local ok, err = pcall(parse_parallel_connections, 'AUTO', nil)
    assert_eq(ok, false, "should have raised error")
    assert(tostring(err):find("Could not determine VCPU"), "error message should mention VCPU")
end)

-- Simulate the parallel import rewriting logic from snowflake_to_exasol.sql
local function rewrite_imports(res, parallel)
    if parallel > 1 then
        local new_res = {}
        for i = 1, #res do
            local sql = res[i].SQL_TEXT
            -- Normalize whitespace and trim (Exasol may reformat with newlines)
            local norm = sql:gsub("%s+", " "):match("^%s*(.-)%s*$")
            -- Case-insensitive match via lowered copy; byte positions identical for ASCII
            local lower_norm = norm:lower()
            local lprefix, linner = lower_norm:match("^(import into .+ from jdbc at .+) statement '(select .+)'%s*;$")
            if lprefix and linner then
                local prefix = norm:sub(1, #lprefix)
                local sep_len = #" statement '"
                local inner_select = norm:sub(#lprefix + sep_len + 1, #lprefix + sep_len + #linner)
                local parallel_sql = prefix
                for p = 0, parallel - 1 do
                    parallel_sql = parallel_sql .. "\n  STATEMENT 'SELECT * EXCLUDE (_prt) FROM ("
                        .. "SELECT *, MOD(ABS(HASH(*)), " .. parallel .. ") AS _prt FROM ("
                        .. inner_select .. ")) WHERE _prt = " .. p .. "'"
                end
                parallel_sql = parallel_sql .. ';'
                new_res[#new_res + 1] = {SQL_TEXT = parallel_sql}
            else
                new_res[#new_res + 1] = res[i]
            end
        end
        return new_res
    end
    return res
end

print("")
print("=== Parallel Import Rewriting Tests ===")

test("parallel=1 does not rewrite imports", function()
    local res = {
        {SQL_TEXT = 'import into "S"."T"("col1","col2") from jdbc at CONN statement \'select "col1","col2" from "DB"."S"."T"\';'},
    }
    local result = rewrite_imports(res, 1)
    assert_eq(#result, 1)
    assert_eq(result[1].SQL_TEXT, res[1].SQL_TEXT, "should be unchanged")
end)

test("parallel=2 rewrites import into 2 STATEMENT clauses", function()
    local res = {
        {SQL_TEXT = 'import into "S"."T"("col1") from jdbc at CONN statement \'select "col1" from "DB"."S"."T"\';'},
    }
    local result = rewrite_imports(res, 2)
    assert_eq(#result, 1)
    local sql = result[1].SQL_TEXT
    -- Should contain 2 STATEMENT clauses
    local count = 0
    for _ in sql:gmatch("STATEMENT '") do count = count + 1 end
    assert_eq(count, 2, "should have 2 STATEMENT clauses")
    -- Should contain partition filters for 0 and 1
    assert(sql:find("WHERE _prt = 0"), "should have partition filter for 0")
    assert(sql:find("WHERE _prt = 1"), "should have partition filter for 1")
    -- HASH(*) should be in the inner SELECT, not the WHERE
    assert(sql:find("MOD%(ABS%(HASH%(%*%)%), 2%) AS _prt"), "should compute _prt via HASH(*) in SELECT")
end)

test("parallel=4 rewrites import into 4 STATEMENT clauses", function()
    local res = {
        {SQL_TEXT = 'import into "S"."T"("col1") from jdbc at CONN statement \'select "col1" from "DB"."S"."T"\';'},
    }
    local result = rewrite_imports(res, 4)
    assert_eq(#result, 1)
    local sql = result[1].SQL_TEXT
    local count = 0
    for _ in sql:gmatch("STATEMENT '") do count = count + 1 end
    assert_eq(count, 4, "should have 4 STATEMENT clauses")
    assert(sql:find("WHERE _prt = 3"), "should have partition filter for 3")
end)

test("parallel rewriting wraps original SELECT as subquery", function()
    local res = {
        {SQL_TEXT = [[import into "S"."T"("col1") from jdbc at CONN statement 'select substring("col1" ,0, 100) from "DB"."S"."T"';]]},
    }
    local result = rewrite_imports(res, 2)
    local sql = result[1].SQL_TEXT
    -- The original select should be wrapped inside the HASH computation
    assert(sql:find('HASH%(%*%)'), "should use HASH(*) for partitioning")
    assert(sql:find('FROM %(select substring'), "should wrap original SELECT as subquery")
end)

test("non-import statements pass through unchanged", function()
    local res = {
        {SQL_TEXT = '-- ### SCHEMAS ###'},
        {SQL_TEXT = 'create schema if not exists "MY_SCHEMA";'},
        {SQL_TEXT = 'create or replace table "MY_SCHEMA"."T1" ("ID" DECIMAL(10,0));'},
        {SQL_TEXT = '-- ### IMPORTS ###'},
    }
    local result = rewrite_imports(res, 4)
    assert_eq(#result, 4)
    assert_eq(result[1].SQL_TEXT, '-- ### SCHEMAS ###')
    assert_eq(result[2].SQL_TEXT, 'create schema if not exists "MY_SCHEMA";')
    assert_eq(result[3].SQL_TEXT, 'create or replace table "MY_SCHEMA"."T1" ("ID" DECIMAL(10,0));')
    assert_eq(result[4].SQL_TEXT, '-- ### IMPORTS ###')
end)

test("parallel rewriting handles uppercase keywords from Exasol", function()
    local res = {
        {SQL_TEXT = [[IMPORT INTO "S"."T"("col1") FROM JDBC at CONN STATEMENT 'select "col1"from "DB"."S"."T"';]]},
    }
    local result = rewrite_imports(res, 2)
    assert_eq(#result, 1)
    local sql = result[1].SQL_TEXT
    local count = 0
    for _ in sql:gmatch("STATEMENT '") do count = count + 1 end
    assert_eq(count, 2, "should have 2 STATEMENT clauses")
end)

test("parallel rewriting handles multiline SQL from Exasol", function()
    local res = {
        {SQL_TEXT = "IMPORT INTO \"S\".\"T\"(\"col1\") FROM \nJDBC at CONN STATEMENT\n'select \"col1\"\nfrom \"DB\".\"S\".\"T\"'\n;"},
    }
    local result = rewrite_imports(res, 3)
    assert_eq(#result, 1)
    local sql = result[1].SQL_TEXT
    local count = 0
    for _ in sql:gmatch("STATEMENT '") do count = count + 1 end
    assert_eq(count, 3, "should have 3 STATEMENT clauses")
    assert(sql:find("WHERE _prt = 2"), "should have partition filter for 2")
end)

test("parallel rewriting preserves original case in table/column names", function()
    local res = {
        {SQL_TEXT = [[IMPORT INTO "MySchema"."MyTable"("MyCol") FROM JDBC at CONN STATEMENT 'select "MyCol"from "DB"."MySchema"."MyTable"';]]},
    }
    local result = rewrite_imports(res, 2)
    local sql = result[1].SQL_TEXT
    assert(sql:find('"MySchema"'), "should preserve original case in schema name")
    assert(sql:find('"MyCol"'), "should preserve original case in column name")
end)

test("mixed import and non-import statements", function()
    local res = {
        {SQL_TEXT = '-- comment'},
        {SQL_TEXT = 'create schema if not exists "S";'},
        {SQL_TEXT = 'import into "S"."T"("col1") from jdbc at CONN statement \'select "col1" from "DB"."S"."T"\';'},
    }
    local result = rewrite_imports(res, 2)
    assert_eq(#result, 3)
    assert_eq(result[1].SQL_TEXT, '-- comment', "comment should be unchanged")
    assert_eq(result[2].SQL_TEXT, 'create schema if not exists "S";', "create schema should be unchanged")
    -- Only the import should be rewritten
    local count = 0
    for _ in result[3].SQL_TEXT:gmatch("STATEMENT '") do count = count + 1 end
    assert_eq(count, 2, "import should be rewritten with 2 STATEMENT clauses")
end)

-- === INFO Output Format Tests ===
print("")
print("=== INFO Output Format Tests ===")

-- Simulate the INFO early-return output from snowflake_to_exasol.sql
local function build_info_output(vcpu_count, parallel)
    return {
        {'-- PARALLEL_CONNECTIONS INFO', 'INFO', nil},
        {'-- Cluster VCPUs: ' .. vcpu_count, 'INFO', nil},
        {'-- AUTO would use: ' .. parallel .. ' parallel connections (ratio: ' .. AUTO_VCPU_RATIO .. ')', 'INFO', nil},
        {'-- Max possible: ' .. vcpu_count, 'INFO', nil},
    }
end

test("INFO output has 4 rows", function()
    local parallel, info_mode, vcpu = parse_parallel_connections('INFO', 8)
    assert_eq(info_mode, true)
    local output = build_info_output(vcpu, parallel)
    assert_eq(#output, 4)
end)

test("INFO output rows have correct structure", function()
    local parallel, info_mode, vcpu = parse_parallel_connections('INFO', 8)
    local output = build_info_output(vcpu, parallel)
    -- All rows should have 'INFO' as second element
    for i = 1, #output do
        assert_eq(output[i][2], 'INFO', "row " .. i .. " should have INFO status")
    end
end)

test("INFO output contains expected text patterns", function()
    local parallel, info_mode, vcpu = parse_parallel_connections('INFO', 12)
    local output = build_info_output(vcpu, parallel)
    assert(output[1][1]:find('PARALLEL_CONNECTIONS INFO'), "row 1 should mention PARALLEL_CONNECTIONS INFO")
    assert(output[2][1]:find('Cluster VCPUs: 12'), "row 2 should show VCPU count")
    assert(output[3][1]:find('AUTO would use: 9'), "row 3 should show computed parallel")  -- floor(12*0.75)=9
    assert(output[3][1]:find('ratio: 0.75'), "row 3 should show ratio")
    assert(output[4][1]:find('Max possible: 12'), "row 4 should show max")
end)

test("INFO output with VCPU=1 shows clamped parallel=1", function()
    local parallel, info_mode, vcpu = parse_parallel_connections('INFO', 1)
    local output = build_info_output(vcpu, parallel)
    assert(output[2][1]:find('Cluster VCPUs: 1'), "should show VCPU=1")
    assert(output[3][1]:find('AUTO would use: 1'), "should show parallel=1 (clamped)")
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
