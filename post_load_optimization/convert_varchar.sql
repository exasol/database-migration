CREATE SCHEMA IF NOT EXISTS DATABASE_MIGRATION;

/*
    convert_varchar - suggest a better data type for VARCHAR columns
    ====================================================================================================
    This script inspects the VARCHAR columns that match the given schema/table filter and, based on a
    SAMPLE of the actual values, suggests the smallest/most appropriate data type for each column. It
    detects: integer / decimal / double, DATE / TIMESTAMP, BOOLEAN, INTERVAL DAY TO SECOND,
    INTERVAL YEAR TO MONTH and GEOMETRY; if no single type fits, it suggests shrinking the VARCHAR
    width (keeping the original character set). For columns whose NAME looks like a date/timestamp but
    whose values do not parse, it prints hints.

    Each column is analyzed with ONE lean aggregate query over a sample: a short-circuiting CASE classifies
    every value exactly once (a numeric value is settled by IS_NUMBER alone, without running the costly
    date/timestamp/interval/geometry checks), so the per-row cost matches the column's real type. The
    NLS-independent multi-format probe is a separate query that runs only for the few columns left
    unclassified. For large tables prefer a sample over '100%' -- a 1-5% sample is usually statistically
    sufficient for a reliable result and is much faster.

    Only real, local base TABLE columns are inspected (views/synonyms and VIRTUAL SCHEMA columns are
    excluded).

    ----------------------------------------------------------------------------------------------------
    REPORT ONLY - this script does NOT change anything. It only RETURNS rows: a "conversion" description,
    the "query_text" you could run, and "notes" (warnings/hints). You execute the statements yourself.

    !!! REVIEW BEFORE YOU APPLY ANYTHING !!!
      - Decisions are based on a SAMPLE (see sample_size). A value OUTSIDE the sample may not fit the
        suggested type, so a generated ALTER can still fail or, worse, change data.
      - Numeric/date suggestions can be LOSSY: e.g. '007' -> 7 (leading zeros lost), '+49' -> 49
        (sign/format lost), zip codes / phone numbers / article numbers are typical traps.
      - Always review each statement and, ideally, run them one by one and check the result.
    ----------------------------------------------------------------------------------------------------

    Output columns (sorted by schema_name, table_name, column_name):
      schema_name, table_name, column_name : the inspected column
      conversion  : short description of the suggestion, e.g. "VARCHAR(2000000) UTF8 --> DECIMAL(9, 0)" or
                    "Keep VARCHAR(100) UTF8, max length: 12"
      query_text  : the ALTER statement(s) to run (empty for "keep"/advisory rows). Multi-format
                    date/timestamp suggestions include the required "ALTER SESSION SET NLS_..._FORMAT".
      notes       : warnings (leading zeros / '+' sign), 0/1-boolean note, ambiguity, precision recipe, hints

    Parameters:
      schema_pattern      : schema name or filter, wildcards (%) allowed
      table_pattern       : table name or filter, wildcards (%) allowed
      sample_size         : table data sample size; either an integer (number of rows, min 1000) or a
                            string expressing an integer percentage, e.g. '5%'. Anything else defaults to 1%.
                            A 1-5% sample is usually statistically sufficient for a reliable type guess and
                            is much faster on large tables; use '100%' only when you must check every value.
      log_for_all_columns : false = only report columns that get a suggestion (a conversion or shrink);
                            true  = report every inspected VARCHAR column (incl. "Keep ..." rows)

    No Animals Were Harmed in the Making of This Script.
*/

--parameter	schema_pattern:      schema name or SCHEMA filter (can be %)
--parameter	table_pattern:       table name or TABLE filter (can be %)
--parameter	sample_size:         number of rows (min 1000) or a percentage string like '5%'
--parameter	log_for_all_columns: false = only report columns that change, true = report every inspected column
--/
CREATE OR REPLACE SCRIPT database_migration.convert_varchar(schema_pattern, table_pattern, sample_size, log_for_all_columns) RETURNS TABLE AS

    local sample_rows = 0
    local sample_pct  = 0
    local sample_min  = 1000

    function adjust_precision(prec)
        if prec <= 9 then
            return 9
        elseif prec <= 18 then
            return 18
        else
            return 36
        end
    end

    -- VARCHAR shrink size: actual max length + ~20% headroom, rounded up to the next round number,
    -- capped at 2,000,000.
    function estimate_optimal_varchar_length(n)
        local n_p20            = math.ceil(n + n * 0.2)
        local number_digits    = string.len(n_p20)
        local magnitude        = math.floor(10 ^ (number_digits - 1))
        local estimated_length = math.floor(n_p20 / magnitude) * magnitude + magnitude
        return math.min(estimated_length, 2000000)
    end

    -- Reads a single session parameter; returns default_value if the query fails or is empty.
    function param_value(name, default_value)
        local ok, r = pquery([[SELECT session_value FROM exa_parameters WHERE parameter_name = :p]], {p = name})
        if ok and r[1] ~= nil and r[1][1] ~= nil then
            return r[1][1]
        end
        return default_value
    end

    -- Returns the maximal string length found in one column of the result table (>= 1, capped at 2000).
    function getMaxLengthForColumn(input_table, column_number)
        local length = 1
        for i = 1, #input_table do
            local v = input_table[i][column_number]
            if v ~= nil and #v > length then length = #v end
        end
        return math.min(length, 2000)
    end

    -- Date-part candidates for the NLS-independent multi-format detection. "swap" is the day/month-swapped
    -- sibling used for ambiguity detection (e.g. DD.MM vs MM.DD when every day is <= 12).
    local CAND = {
        {fmt='YYYY-MM-DD'}, {fmt='YYYY.MM.DD'}, {fmt='YYYY/MM/DD'},
        {fmt='DD.MM.YYYY', swap='MM.DD.YYYY'}, {fmt='MM.DD.YYYY', swap='DD.MM.YYYY'},
        {fmt='DD/MM/YYYY', swap='MM/DD/YYYY'}, {fmt='MM/DD/YYYY', swap='DD/MM/YYYY'},
        {fmt='DD-MM-YYYY', swap='MM-DD-YYYY'}, {fmt='MM-DD-YYYY', swap='DD-MM-YYYY'}
    }

    -- Multi-format date/timestamp detection via EXPLICIT format models (NLS-independent). It runs its own
    -- probe query and is called ONLY for the few columns the primary type check leaves unclassified (see
    -- the else-branch), so its extra per-row cost (one IS_DATE + one IS_TIMESTAMP per candidate format) is
    -- never paid for the numeric/date/etc. columns that make up most of a database. A format "matches" only
    -- if it parses ALL non-null sampled values; a conversion is proposed only when exactly ONE unambiguous
    -- format matches (an ambiguous day/month order is reported, not guessed).
    -- Returns: nil | {ambiguous=true} | {fmt='DD.MM.YYYY', kind='DATE'|'TIMESTAMP', frac=<0..9>}
    function detect_explicit_format(sch_raw, tab_raw, col_raw, sample, full_scan)
        -- string.char(92) produces the backslash for the trailing-fraction regex without writing a literal
        -- backslash in the Lua source (Lua rejects an invalid escape, and a backslash directly before a
        -- quote is misread by some SQL clients as an escaped quote).
        local sel = "SUM(CASE WHEN col IS NOT NULL THEN 1 ELSE 0 END) entries"
                 .. ", MAX(CASE WHEN INSTR(col, CHR(58)) > 0 THEN 1 ELSE 0 END) has_time"
                 .. ", MAX(COALESCE(LENGTH(REGEXP_SUBSTR(col, '" .. string.char(92) .. ".[0-9]+$')) - 1, 0)) maxfrac"
        for i, c in ipairs(CAND) do
            sel = sel .. ", SUM(CASE WHEN IS_DATE(col, '" .. c.fmt .. "') THEN 1 ELSE 0 END) df" .. i
                      .. ", SUM(CASE WHEN IS_TIMESTAMP(col, '" .. c.fmt .. " HH24:MI:SS.FF9') THEN 1 ELSE 0 END) tf" .. i
        end
        -- Omit the LIMIT subquery on a full scan (the sample covers the whole table), so the probe does
        -- not materialize a temporary copy either; with a real sample the LIMIT keeps it to the sample.
        local from_src = [[ SELECT ::col AS col FROM ::sch.::tab ]]
        local binds    = { sch = quote(sch_raw), tab = quote(tab_raw), col = quote(col_raw) }
        if not full_scan then from_src = from_src .. [[ LIMIT :lmt ]]; binds.lmt = sample end
        local ok, r = pquery([[ SELECT ]] .. sel .. [[ FROM ( ]] .. from_src .. [[ ) ]], binds)
        if not ok then return nil end
        local res3     = r[1]
        local entries  = res3["ENTRIES"]
        if entries == nil or entries == 0 then return nil end
        local has_time = res3["HAS_TIME"] == 1
        local maxfrac  = math.min(res3["MAXFRAC"] or 0, 9)
        local prefix   = has_time and "TF" or "DF"
        local matched  = {}
        local nmatched = 0
        for i, c in ipairs(CAND) do
            if res3[prefix .. i] == entries then matched[c.fmt] = true; nmatched = nmatched + 1 end
        end
        if nmatched == 0 then return nil end
        for i, c in ipairs(CAND) do                       -- ambiguous if a format AND its swap both match
            if matched[c.fmt] and c.swap and matched[c.swap] then return { ambiguous = true } end
        end
        for i, c in ipairs(CAND) do                       -- the single unambiguous match
            if matched[c.fmt] then
                return { fmt = c.fmt, kind = (has_time and 'TIMESTAMP' or 'DATE'), frac = maxfrac }
            end
        end
        return nil
    end

    -- Sorts the rows (schema, table, column), inserts a friendly message if empty, sizes the output
    -- columns dynamically and exits. log_for_all_columns selects which empty message is shown.
    function emit_and_exit(rows)
        table.sort(rows, function(a, b)
            if a[1] ~= b[1] then return a[1] < b[1] end
            if a[2] ~= b[2] then return a[2] < b[2] end
            return a[3] < b[3]
        end)
        if #rows == 0 then
            local empty_msg
            if log_for_all_columns then
                empty_msg = 'No matching VARCHAR columns found (check the schema/table filter).'
            else
                empty_msg = 'No columns found that need optimization.'
            end
            rows[1] = {'', '', '', empty_msg, '', ''}
        end
        local ls  = getMaxLengthForColumn(rows, 1)
        local lt  = getMaxLengthForColumn(rows, 2)
        local lc  = getMaxLengthForColumn(rows, 3)
        local lco = getMaxLengthForColumn(rows, 4)
        local lq  = getMaxLengthForColumn(rows, 5)
        local ln  = getMaxLengthForColumn(rows, 6)
        exit(rows, "schema_name char(" .. ls .. "), table_name char(" .. lt .. "), column_name char(" .. lc
                   .. "), conversion char(" .. lco .. "), query_text char(" .. lq .. "), notes char(" .. ln .. ")")
    end

    -- Check parameters (both schema_pattern AND table_pattern must be non-empty strings)
    if schema_pattern == '' or type(schema_pattern) ~= 'string' or
       table_pattern  == '' or type(table_pattern)  ~= 'string' then
        emit_and_exit({ {'', '', '', 'Invalid parameters: schema_pattern and table_pattern must be non-empty strings.', '', ''} })
    end

    -- Sample size: integer rows (min 1000) or a percentage string like '5%'; default 1%
    if type(sample_size) == 'number' then
        sample_rows = math.max(math.floor(sample_size), sample_min)
    elseif type(sample_size) == 'string' then
        if string.match(sample_size, '^[0-9]+%%$') then
            sample_pct = math.max(math.min(tonumber(string.match(sample_size, '^[0-9]+')), 100), 1)
        elseif string.match(sample_size, '^[0-9]+$') then
            sample_rows = math.max(tonumber(string.match(sample_size, '^[0-9]+')), sample_min)
        else
            sample_pct = 1
        end
    else
        sample_pct = 1
    end

    -- Decimal character (default '.'), and the numeric recognition patterns
    local dec_char = string.sub(param_value('NLS_NUMERIC_CHARACTERS', '.'), 1, 1)
    if dec_char == '' then dec_char = '.' end
    -- The regex pieces below deliberately avoid two characters that some SQL clients (e.g. DbVisualizer)
    -- otherwise mistake for parameter markers when installing the script:
    --   * "{0,1}" is used instead of the regex zero-or-one quantifier (the question-mark character) --
    --     it is equivalent in the Exasol REGEXP_LIKE function.
    --   * the backslash that escapes the decimal separator is produced with string.char(92), not written
    --     as a literal, so the script text never contains a backslash immediately before a quote (a naive
    --     parser reads that as an escaped quote and then loses track of where string literals end).
    local dec_re     = string.char(92) .. dec_char   -- escaped decimal separator (backslash + dec char)
    local regexp_int = '^ *[+-]{0,1}[0-9]+ *$'
    local regexp_dec = '^ *[+-]{0,1}[0-9]*' .. dec_re .. '[0-9]+ *$'
    local regexp_dpr = '^ *[+-]{0,1}[0-9]*(' .. dec_re .. '[0-9]+){0,1}[eE][+-]{0,1}[0-9]+ *$'

    -- Date/timestamp formats (used for the "column name looks like a date" hints and the FF recipe)
    local nls_date_format      = param_value('NLS_DATE_FORMAT',      'YYYY-MM-DD')
    local nls_timestamp_format = param_value('NLS_TIMESTAMP_FORMAT', 'YYYY-MM-DD HH24:MI:SS.FF6')

    -- Fractional-seconds precision the current NLS_TIMESTAMP_FORMAT can PARSE (FF<n>); used to warn
    -- when a column has more fractional digits than a plain ALTER (implicit conversion) would keep.
    local nls_ff = 0
    local ff_digit = string.match(nls_timestamp_format, 'FF(%d)')
    if ff_digit ~= nil then
        nls_ff = tonumber(ff_digit)
    elseif string.find(nls_timestamp_format, 'FF', 1, true) ~= nil then
        nls_ff = 6   -- 'FF' without a digit defaults to 6 in Exasol
    end

    -- Note on NLS: the PRIMARY detection (IS_DATE/IS_TIMESTAMP/IS_NUMBER and the plain ALTERs) uses the
    -- current session NLS settings; additionally, unclassified columns are probed against explicit format
    -- models (see detect_explicit_format), so e.g. a German '12.06.2026' is detected regardless of the NLS.
    -- The date/timestamp split uses IS_DATE / IS_TIMESTAMP (no datetime subtraction), so it is independent
    -- of TIMESTAMP_ARITHMETIC_BEHAVIOR and works even if the date and timestamp formats differ.

    -- Getting the VARCHAR columns (base tables only, no virtual schemas)
    local suc, res = pquery([[
        SELECT column_schema
             , column_table
             , column_name
             , column_maxsize
             , column_type
        FROM   exa_all_columns
        WHERE  column_schema      LIKE :schp
           AND column_table       LIKE :tabp
           AND column_type_id     = 12          -- VARCHAR
           AND column_object_type = 'TABLE'     -- only base tables, never views/synonyms
           AND column_is_virtual  = FALSE       -- never virtual schema columns
        ORDER  BY column_schema
                , column_table
                , column_name
     ]], {
         schp = schema_pattern
       , tabp = table_pattern
     })
    if not suc then
        emit_and_exit({ {'', '', '', 'Could not read EXA_ALL_COLUMNS: ' .. (res.error_message or 'unknown error'), '', ''} })
    end

    local overall_res = {}
    local tab_name    = "-"
    local tab_rows    = 0
    local tab_sample  = 0
    local tab_status  = ""   -- "" = ok; otherwise an "empty"/"could not analyze" message for every column

    -- Iterating through tables and their columns
    for i = 1, #res do
        local curr_sch      = res[i]["COLUMN_SCHEMA"]
        local curr_tab      = res[i]["COLUMN_TABLE"]
        local curr_col      = res[i]["COLUMN_NAME"]
        local curr_col_len  = res[i]["COLUMN_MAXSIZE"]
        local curr_col_type = res[i]["COLUMN_TYPE"]

        if tab_name ~= curr_tab then -- New table: one COUNT(*) per table (needed for percentage sampling)
            tab_name   = curr_tab
            tab_status = ""
            local csuc, cres = pquery([[ SELECT COUNT(*) FROM ::sch.::tab ]],
                                      { sch = quote(curr_sch), tab = quote(curr_tab) })
            if not csuc then
                tab_rows   = 0
                tab_status = "Table could not be analyzed (" .. (cres.error_message or 'error') .. ")"
            else
                tab_rows = cres[1][1]
                if tab_rows == 0 then
                    tab_sample = 0
                    tab_status = "Table is empty (no data)"
                else
                    if sample_rows ~= 0 then        -- sample size given as a number of rows
                        tab_sample = sample_rows
                    else                            -- sample size given as a percentage
                        tab_sample = math.floor(tab_rows * (sample_pct / 100))
                        if tab_sample < sample_min then
                            tab_sample = math.min(tab_rows, sample_min)
                        end
                    end
                end
            end
        end -- new table

        if tab_rows == 0 then
            -- empty table / count failed: nothing to convert; only listed with log_for_all_columns
            if log_for_all_columns then
                overall_res[#overall_res + 1] = { curr_sch, curr_tab, curr_col, tab_status, "", "" }
            end
        else
            -- ONE lean aggregate query per column. A short-circuiting CASE classifies each sampled value
            -- exactly ONCE: a value that IS_NUMBER is settled without ever evaluating the (far more
            -- expensive) date/timestamp/interval/geometry checks. So the per-row work matches the column's
            -- real type instead of running every check on every row -- the key to performance on big tables.
            -- The NLS-independent multi-format probe is NOT folded in here; it runs (see the else-branch)
            -- only for the few columns this query leaves unclassified.
            --
            -- When the sample covers the whole table (e.g. '100%'), the LIMIT subquery is OMITTED: a
            -- "... FROM (SELECT col FROM t LIMIT n)" makes Exasol first copy every row into a temporary
            -- table (a costly INSERT step); without it the base table is scanned straight into the
            -- aggregation. With a real sample the LIMIT stays and materializes only the small sample.
            local from_src  = [[ SELECT ::col AS col FROM ::sch.::tab ]]
            local full_scan = (sample_rows ~= 0 and sample_rows >= tab_rows)
                           or (sample_rows == 0 and sample_pct >= 100)
            local binds = { sch = quote(curr_sch), tab = quote(curr_tab), col = quote(curr_col)
                          , r_int = regexp_int, r_dec = regexp_dec, r_dpr = regexp_dpr, dec = dec_char }
            if not full_scan then
                from_src  = from_src .. [[ LIMIT :lmt ]]
                binds.lmt = tab_sample
            end
            local qsuc, res2 = pquery([[
                SELECT COUNT(col) entries
                     , SUM(CASE WHEN cat = 'INT' THEN 1 ELSE 0 END) its_integer
                     , MAX(CASE WHEN cat = 'INT' THEN LENGTH(LTRIM(TRIM(col), '+-')) ELSE 0 END) its_integer_precision
                     , SUM(CASE WHEN cat = 'DEC' THEN 1 ELSE 0 END) its_decimal
                     , MAX(CASE WHEN cat = 'DEC' THEN LENGTH(LTRIM(TRIM(SUBSTR(col, 1, INSTR(col, :dec) - 1), '+-'))) ELSE 0 END) its_decimal_precision
                     , MAX(CASE WHEN cat = 'DEC' THEN LENGTH(LTRIM(TRIM(SUBSTR(col, INSTR(col, :dec) + 1), '+-'))) ELSE 0 END) its_decimal_scale
                     , SUM(CASE WHEN cat = 'DBL' THEN 1 ELSE 0 END) its_double_precision
                     -- numeric values that look like identifiers: a leading zero (007, 0301234) or a leading
                     -- '+' (+49170) -> a numeric conversion would silently lose them. Checked only for numerics.
                     , SUM(CASE WHEN cat IN ('INT', 'DEC')
                                 AND (TRIM(col) REGEXP_LIKE '[+-]{0,1}0[0-9].*' OR TRIM(col) REGEXP_LIKE '\+.*')
                                THEN 1 ELSE 0 END) its_numeric_idlike
                     , SUM(CASE WHEN cat = 'DATE' THEN 1 ELSE 0 END) its_date
                     , SUM(CASE WHEN cat = 'TS'   THEN 1 ELSE 0 END) its_timestamp
                     , MAX(CASE WHEN cat = 'TS' AND INSTR(col, '.') > 0
                                THEN LENGTH(RTRIM(SUBSTR(col, INSTR(col, '.') + 1))) ELSE 0 END) its_timestamp_fp
                     -- IS_BOOLEAN is evaluated directly (not via cat) so the boolean count is byte-for-byte
                     -- what the previous version computed -- IS_BOOLEAN also accepts numeric 0/1/00/01, which
                     -- a cat-based count would miss. (Numeric values are still classified INT/DEC/DBL above;
                     -- this only adds the boolean tally needed for the ENTRIES = ITS_BOOLEAN branch.)
                     , SUM(CASE WHEN IS_BOOLEAN(col) THEN 1 ELSE 0 END) its_boolean
                     , SUM(CASE WHEN col IN ('0', '1') THEN 1 ELSE 0 END) its_boolean_binary
                     , SUM(CASE WHEN cat = 'DSI' THEN 1 ELSE 0 END) its_dsinterval
                     , MAX(CASE WHEN cat = 'DSI' THEN LENGTH(LTRIM(SUBSTR(col, 1, INSTR(col, ' ') - 1), '+-')) ELSE 0 END) its_dsinterval_p
                     , MAX(CASE WHEN cat = 'DSI' AND INSTR(col, '.') > 0 THEN LENGTH(SUBSTR(col, INSTR(col, '.') + 1)) ELSE 0 END) its_dsinterval_fp
                     , SUM(CASE WHEN cat = 'YMI' THEN 1 ELSE 0 END) its_yminterval
                     , MAX(CASE WHEN cat = 'YMI' THEN LENGTH(LTRIM(SUBSTR(col, 1, INSTR(col, '-') - 1), '+-')) ELSE 0 END) its_yminterval_p
                     , SUM(CASE WHEN cat = 'GEO' THEN 1 ELSE 0 END) its_geometry
                     , MAX(LENGTH(col)) max_length
                     -- cheap pre-check (one regex, only on UNCLASSIFIED values): whether they even look
                     -- date-like (digits + . : / - separators). If not (names, codes, free text), the
                     -- expensive multi-format probe in the else-branch is skipped entirely.
                     , MAX(CASE WHEN cat = 'OTH' AND col REGEXP_LIKE '^ *[0-9][0-9 .:/-]*[0-9] *$'
                                THEN 1 ELSE 0 END) maybe_dateish
                FROM (
                    -- classify each value once; the WHENs are ordered cheapest/most-common first and the
                    -- CASE stops at the first match (Exasol evaluates WHEN conditions in order).
                    SELECT col
                         , CASE
                               WHEN IS_NUMBER(col) THEN
                                    CASE WHEN col REGEXP_LIKE :r_int THEN 'INT'
                                         WHEN col REGEXP_LIKE :r_dec THEN 'DEC'
                                         WHEN col REGEXP_LIKE :r_dpr THEN 'DBL'
                                         ELSE 'OTH' END
                               -- TO_TIMESTAMP only runs when IS_TIMESTAMP is true (AND short-circuits); TRUNC
                               -- detects a time component independent of TIMESTAMP_ARITHMETIC_BEHAVIOR.
                               WHEN IS_TIMESTAMP(col) AND TO_TIMESTAMP(col) <> TRUNC(TO_TIMESTAMP(col)) THEN 'TS'
                               WHEN IS_DATE(col) THEN 'DATE'
                               WHEN IS_DSINTERVAL(col) THEN 'DSI'
                               WHEN IS_YMINTERVAL(col) THEN 'YMI'
                               WHEN UPPER(col) REGEXP_LIKE '.*(POINT|LINESTRING|LINEARRING|POLYGON|GEOMETRYCOLLECTION|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON) *\(.*' THEN 'GEO'
                               ELSE 'OTH'
                           END cat
                    FROM ( ]] .. from_src .. [[ )
                )
            ]], binds)

            local conversion = ""
            local query_text = ""
            local notes      = ""

            -- Exact current data type for the CONVERSION field. This script only inspects VARCHAR columns
            -- (column_type_id = 12), so the source is always "VARCHAR(<length>) <charset>". The character set
            -- (ASCII/UTF8) is kept for both the source-type label and any VARCHAR width shrink.
            local charset = 'UTF8'
            if string.find(string.upper(curr_col_type), 'ASCII', 1, true) ~= nil then
                charset = 'ASCII'
            end
            local src_type = "VARCHAR(" .. curr_col_len .. ") " .. charset

            if not qsuc then
                -- Do not abort the whole run because of one problematic column; always surface the error.
                conversion = "Could not analyze " .. src_type
                notes      = res2.error_message or 'error'
                overall_res[#overall_res + 1] = { curr_sch, curr_tab, curr_col, conversion, query_text, notes }
            else
                local r       = res2[1]
                local ucol    = string.upper(curr_col)
                local sch_q   = quote(curr_sch)
                local tab_q   = quote(curr_tab)
                local col_q   = quote(curr_col)
                local altpfx  = "ALTER TABLE " .. sch_q .. "." .. tab_q .. " MODIFY COLUMN " .. col_q .. " "

                if r["ENTRIES"] == 0 then -- No data in the (sampled) column
                    conversion = "Keep " .. src_type .. " (no data in sample - candidate for DROP COLUMN)"
                    notes      = "Column had no data in the sample - it may be a candidate for DROP COLUMN. Verify before dropping."
                elseif r["ENTRIES"] == r["ITS_INTEGER"] then -- all sample values were integers
                    if r["ENTRIES"] == r["ITS_BOOLEAN_BINARY"] then -- ...but could be a 0/1 boolean
                        conversion = src_type .. " --> BOOLEAN"
                        query_text = altpfx .. "BOOLEAN;"
                        notes      = "NOTE: only 0/1 values. Verify these are real booleans, not flags/bits/codes you compute with."
                    elseif r["ITS_INTEGER_PRECISION"] > 36 then
                        conversion = "Keep " .. src_type .. " (integer precision " .. r["ITS_INTEGER_PRECISION"] .. " > max 36)"
                        notes      = "Largest integer precision " .. r["ITS_INTEGER_PRECISION"] .. " exceeds the maximum DECIMAL precision of 36; not convertible."
                    else
                        conversion = src_type .. " --> DECIMAL(" .. adjust_precision(r["ITS_INTEGER_PRECISION"]) .. ", 0)"
                        query_text = altpfx .. "DECIMAL(" .. adjust_precision(r["ITS_INTEGER_PRECISION"]) .. ", 0);"
                        if r["ITS_NUMERIC_IDLIKE"] > 0 then
                            notes = "WARNING: some values have leading zeros or a '+' sign (looks like an identifier: ID / ZIP / phone / article no.). Converting to DECIMAL LOSES them ('007' -> 7, '+49' -> 49). Review before applying!"
                        end
                    end
                elseif r["ENTRIES"] == r["ITS_INTEGER"] + r["ITS_DECIMAL"] then -- integers and decimals (not scientific)
                    local total_precision = math.max(r["ITS_INTEGER_PRECISION"], r["ITS_DECIMAL_PRECISION"]) + r["ITS_DECIMAL_SCALE"]
                    if total_precision > 36 then
                        conversion = "Keep " .. src_type .. " (decimal precision " .. total_precision .. " > max 36)"
                        notes      = "Largest decimal precision " .. total_precision .. " exceeds the maximum DECIMAL precision of 36; not convertible."
                    else
                        conversion = src_type .. " --> DECIMAL(" .. total_precision .. ", " .. r["ITS_DECIMAL_SCALE"] .. ")"
                        query_text = altpfx .. "DECIMAL(" .. total_precision .. ", " .. r["ITS_DECIMAL_SCALE"] .. ");"
                        if r["ITS_NUMERIC_IDLIKE"] > 0 then
                            notes = "WARNING: some values have leading zeros or a '+' sign (looks like an identifier). Converting to DECIMAL LOSES them. Review before applying!"
                        end
                    end
                elseif r["ENTRIES"] == r["ITS_INTEGER"] + r["ITS_DECIMAL"] + r["ITS_DOUBLE_PRECISION"] then -- any numeric (incl. scientific)
                    conversion = src_type .. " --> DOUBLE PRECISION"
                    query_text = altpfx .. "DOUBLE PRECISION;"
                elseif r["ENTRIES"] == r["ITS_DATE"] then -- dates only (no time component)
                    conversion = src_type .. " --> DATE"
                    query_text = altpfx .. "DATE;"
                elseif r["ENTRIES"] == r["ITS_DATE"] + r["ITS_TIMESTAMP"] then -- dates and/or timestamps
                    local fp = math.min(r["ITS_TIMESTAMP_FP"], 9)
                    conversion = src_type .. " --> TIMESTAMP(" .. fp .. ")"
                    query_text = altpfx .. "TIMESTAMP(" .. fp .. ");"
                    notes      = "Consider TIMESTAMP(" .. fp .. ") WITH LOCAL TIME ZONE if that fits better."
                    if fp > nls_ff then
                        -- a plain ALTER parses via NLS_TIMESTAMP_FORMAT (FF<nls_ff>) and would truncate the
                        -- extra fractional digits; prepend the session-format change that keeps full precision.
                        local rec_fmt = string.gsub(nls_timestamp_format, 'FF%d*', 'FF' .. fp)
                        if rec_fmt == nls_timestamp_format then rec_fmt = nls_timestamp_format .. '.FF' .. fp end
                        query_text = "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='" .. rec_fmt .. "'; " .. query_text
                        notes = notes .. " Values have up to " .. fp .. " fractional-second digits but NLS_TIMESTAMP_FORMAT ('" .. nls_timestamp_format .. "') keeps fewer; the ALTER SESSION in query_text preserves full precision (a plain ALTER would truncate)."
                    end
                elseif r["ENTRIES"] == r["ITS_BOOLEAN"] then -- booleans
                    conversion = src_type .. " --> BOOLEAN"
                    query_text = altpfx .. "BOOLEAN;"
                    notes      = "NOTE: verify the column is really a boolean (not a status text you rely on)."
                elseif r["ENTRIES"] == r["ITS_DSINTERVAL"] then -- day-to-second intervals
                    local p  = math.min(math.max(r["ITS_DSINTERVAL_P"], 1), 9)
                    local fp = math.min(r["ITS_DSINTERVAL_FP"], 9)
                    conversion = src_type .. " --> INTERVAL DAY(" .. p .. ") TO SECOND(" .. fp .. ")"
                    query_text = altpfx .. "INTERVAL DAY(" .. p .. ") TO SECOND(" .. fp .. ");"
                elseif r["ENTRIES"] == r["ITS_YMINTERVAL"] then -- year-to-month intervals
                    local p = math.min(math.max(r["ITS_YMINTERVAL_P"], 1), 9)
                    conversion = src_type .. " --> INTERVAL YEAR(" .. p .. ") TO MONTH"
                    query_text = altpfx .. "INTERVAL YEAR(" .. p .. ") TO MONTH;"
                elseif r["ENTRIES"] == r["ITS_GEOMETRY"] then -- geospatial data
                    conversion = src_type .. " --> GEOMETRY"
                    query_text = altpfx .. "GEOMETRY;"
                    notes      = "Consider specifying an SRID (a reference coordinate system; query EXA_SPATIAL_REF_SYS for possible values)."
                else
                    -- Not classified above. Try (1) multi-format date/timestamp detection (its own probe
                    -- query, run only for unclassified columns whose values are date-LIKE in length and
                    -- shape -- see maybe_dateish; this keeps the probe off name/code/free-text columns),
                    -- (2) column-name hints, (3) width shrink, (4) keep as is.
                    local mf = nil
                    if r["MAX_LENGTH"] >= 6 and r["MAX_LENGTH"] <= 40 and r["MAYBE_DATEISH"] == 1 then
                        mf = detect_explicit_format(curr_sch, curr_tab, curr_col, tab_sample, full_scan)
                    end
                    if mf ~= nil and mf.ambiguous then
                        conversion = "Keep " .. src_type .. " (ambiguous date format DD.MM vs MM.DD - not converted)"
                        notes      = "Values look like dates but the day/month order is ambiguous (every day is <= 12). Pick the format yourself, e.g. ALTER SESSION SET NLS_DATE_FORMAT='DD.MM.YYYY'; then " .. altpfx .. "DATE;"
                    elseif mf ~= nil and mf.kind == 'DATE' then
                        conversion = src_type .. " --> DATE (format " .. mf.fmt .. ")"
                        query_text = "ALTER SESSION SET NLS_DATE_FORMAT='" .. mf.fmt .. "'; " .. altpfx .. "DATE;"
                        notes      = "Values match the date format '" .. mf.fmt .. "' (not the session NLS_DATE_FORMAT). Run BOTH statements in query_text (the ALTER SESSION first)."
                    elseif mf ~= nil and mf.kind == 'TIMESTAMP' then
                        local fp    = mf.frac
                        local tsfmt = mf.fmt .. ' HH24:MI:SS'
                        if fp > 0 then tsfmt = tsfmt .. '.FF' .. fp end
                        conversion = src_type .. " --> TIMESTAMP(" .. fp .. ") (format " .. tsfmt .. ")"
                        query_text = "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='" .. tsfmt .. "'; " .. altpfx .. "TIMESTAMP(" .. fp .. ");"
                        notes      = "Values match the timestamp format '" .. tsfmt .. "' (not the session NLS_TIMESTAMP_FORMAT). Run BOTH statements in query_text (the ALTER SESSION first)."
                    elseif string.match(ucol, "_TIMESTAMP") or string.match(ucol, "TIMESTAMP$") or
                           string.match(ucol, "_TS") or string.match(ucol, "TS$") or
                           string.match(ucol, "_DATETIME") or string.match(ucol, "DATETIME$") or
                           ucol == "TIMSTAMP" then -- name looks like a timestamp
                        conversion = "Keep " .. src_type .. " (name looks like a timestamp; values do not match " .. nls_timestamp_format .. ")"
                        notes      = "If this is a timestamp, normalize then convert, e.g.: UPDATE " .. sch_q .. "." .. tab_q .. " SET " .. col_q .. " = TO_CHAR(TO_TIMESTAMP(" .. col_q .. ", '<timestamp_format>'), '" .. nls_timestamp_format .. "'); then " .. altpfx .. "TIMESTAMP;"
                    elseif string.match(ucol, "_DATE") or string.match(ucol, "DATE$") or string.match(ucol, "^DATE_") or
                           string.match(ucol, "_DT") or string.match(ucol, "DT$") or
                           ucol == "DATE" or ucol == 'DOB' then -- name looks like a date
                        conversion = "Keep " .. src_type .. " (name looks like a date; values do not match " .. nls_date_format .. ")"
                        notes      = "If this is a date, normalize then convert, e.g.: UPDATE " .. sch_q .. "." .. tab_q .. " SET " .. col_q .. " = TO_CHAR(TO_DATE(" .. col_q .. ", '<date_format>'), '" .. nls_date_format .. "'); then " .. altpfx .. "DATE;"
                    elseif estimate_optimal_varchar_length(r["MAX_LENGTH"]) < curr_col_len then -- shrink width
                        local new_col_len = estimate_optimal_varchar_length(r["MAX_LENGTH"])
                        conversion = src_type .. " --> VARCHAR(" .. new_col_len .. ") " .. charset .. ", max length: " .. r["MAX_LENGTH"]
                        query_text = altpfx .. "VARCHAR(" .. new_col_len .. ") " .. charset .. ";"
                        notes      = "Mixed values; no single type fits. Shrinking the width (actual max length " .. r["MAX_LENGTH"] .. " + ~20% headroom); character set preserved."
                    else
                        conversion = "Keep " .. src_type .. ", max length: " .. r["MAX_LENGTH"]
                    end
                end

                if query_text ~= "" or log_for_all_columns then
                    overall_res[#overall_res + 1] = { curr_sch, curr_tab, curr_col, conversion, query_text, notes }
                end
            end -- qsuc

        end -- if tab_rows == 0

    end -- for i = 1, #res

    emit_and_exit(overall_res)
/

-- ====================================================================================================
-- REPORT ONLY - this prints suggestions; it does NOT change anything. REVIEW every statement before
-- you run it (sample-based; numeric/date conversions can be lossy, e.g. '007' -> 7).
-- ====================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.CONVERT_VARCHAR(
    'MY_SCHEMA'    -- schema_pattern: schema name or schema name filter (can be %)
  , '%'            -- table_pattern: table name or table name filter (can be %)
  , '5%'           -- sample_size: rows (min 1000) or a percentage like '5%'. A 1-5% sample is usually
                   --              enough for a reliable result; '100%' scans the whole table (slower)
  , false          -- log_for_all_columns: false = only columns that change, true = every inspected column
);
