CREATE SCHEMA IF NOT EXISTS DATABASE_MIGRATION;

/*
    convert_varchar - suggest a better data type for VARCHAR columns
    ====================================================================================================
    REPORT ONLY - this script does NOT change anything. It returns rows with a description ("conversion"),
    the statement(s) you could run ("query_text") and warnings / hints ("notes"). You run the statements
    yourself, after reviewing them.

    WHAT IT DOES
      For every VARCHAR column that matches the schema/table filter, the values are classified and the
      smallest fitting type is suggested: DECIMAL(p,s), DOUBLE PRECISION, DATE, TIMESTAMP(p), BOOLEAN,
      INTERVAL DAY TO SECOND, INTERVAL YEAR TO MONTH or GEOMETRY. If no single type fits, a smaller VARCHAR
      width is suggested (the character set ASCII / UTF8 is kept). Only VARCHAR columns of local base tables
      are inspected: CHAR columns, views, synonyms and VIRTUAL SCHEMA columns are not.

    HOW THE DECISION IS MADE
      1. Sample. The values are classified on a RANDOM sample per table (WHERE RANDOM() < p, capped at the
         requested number of rows), or on the whole table for '100%', whenever the requested sample is at
         least as large as the table, and for every table with at most 500,000 rows when the requested sample
         covers at least 5% of it (on the reference cluster a full scan was not slower than a 5% sample at
         250,000 and 500,000 rows; a smaller sample keeps the sampled path; the suggestion is the same and the
         notes say so). A random sample finds
         rare values far better than a block of rows, but it is not reproducible and it can never prove that
         every value fits.
      2. Full-table verification. Every conversion proposed from a sample is checked against the FULL column
         (plus the column DEFAULT): the values that do not fit the proposed type are counted and the decisive
         facts are measured again (integer digits, decimals, fractional-second digits, date or timestamp,
         two-digit years, day/month ambiguity, the explicit date format). Size facts are always taken from
         the full column, so nothing is rounded or truncated. If any value does not fit the proposed type,
         the column is classified again over the full column (exactly as with '100%') and a note says how
         many values did not fit the sampled proposal. When the values of the full column do not all fit
         one type, the result is the VARCHAR shrink (see below), never a plain Keep. The suggested conversion
         (conversion and query_text) therefore does not depend on the random draw; only such notes describe
         what the sample saw.
      The width of a VARCHAR shrink and the "no data" check always use the full column.
      Cost per column: one classification query (over the sample, or the whole table) and, when a sample is
      used, one verification query over the full column per proposed conversion; when the verification
      fails, one more classification over the full column. The full-column COUNT / MAX(LENGTH) of a sampled
      table is computed for up to 20 columns in one statement. A column that mixes dates and timestamps gets
      one more check of the target TIMESTAMP format. The multi-format date probe runs only for unclassified
      columns whose values look date-like. Each table is counted once (COUNT(*)). A sampled run can
      therefore read a column more than once. The time per value grows linearly with its length, also for
      values of up to 2,000,000 characters (e.g. long runs of blanks).
      Statements that read at least 20,000 rows (a statement over the sample: the sample rows; a statement
      over the full column: the table rows) are evaluated in a faster way with identical results: per table
      a few hint statements (distinct-value estimate, a text value that rules out every type), then per
      column a classification of the DISTINCT values where there are few, a reduced query for text columns,
      checks skipped where their result is known (GEOMETRY, IS_BOOLEAN, date parsers on long texts), and a
      date probe over the distinct values (from 100,000 rows on, formats that fail on the first 1000
      distinct values are not checked further).

    DETECTED FORMATS
      - Numbers in the session NLS_NUMERIC_CHARACTERS: '5', '-1.25', '.5', '5.' (DECIMAL) and '1E3' (DOUBLE).
      - DATE / TIMESTAMP in the session NLS_DATE_FORMAT / NLS_TIMESTAMP_FORMAT. When the session formats do
        not fit, these explicit formats are probed and used only when exactly ONE of them parses every value:
        YYYY-MM-DD, YYYY.MM.DD, YYYY/MM/DD, DD.MM.YYYY, MM.DD.YYYY, DD/MM/YYYY, MM/DD/YYYY, DD-MM-YYYY,
        MM-DD-YYYY, each also followed by " HH24:MI:SS[.FFn]", and the ISO 8601 form YYYY-MM-DDTHH24:MI:SS[.FFn].
        A value with a time part (also 00:00:00, or an hour alone) is never read as a DATE, because a DATE keeps
        no time of day: it is read as a TIMESTAMP when the session TIMESTAMP format or an explicit format parses
        it, otherwise the column stays text. When every such value of the column (and the DEFAULT) is at
        midnight (00:00:00, a fraction only of zeros; plain dates may be mixed in), the column still becomes a
        DATE: query_text reads every value with that TIMESTAMP format (MODIFY COLUMN ... TIMESTAMP) and then
        changes the column to DATE, which drops only the time 00:00:00. Any other time of day gives a TIMESTAMP.
        When NLS_DATE_FORMAT itself has time elements (e.g. 'MM/DD/YYYY HH12:MI:SS AM'), that rule is not used:
        a value is a DATE only when that format reads it at midnight (e.g. '01/13/2000 12:00:00 AM'). (IS_DATE
        also accepts a trailing time in some formats, e.g. '01/02/2000 10:00:01' under 'MM/DD/YYYY'; a time
        without AM/PM does not fit 'MM/DD/YYYY HH12:MI:SS AM'.)
      - BOOLEAN: every text IS_BOOLEAN accepts (on Exasol 2025.1 e.g. TRUE/FALSE, T/F, Y/N, YES/NO, 1/0,
        00/01, case-insensitive); different texts are merged into TRUE / FALSE (see the note).
      - PRIMARY KEY columns (also in a composite PRIMARY KEY) are kept when different texts would give
        duplicate keys after the conversion: 'Y' and '1' both become TRUE; '1', ' 1', '01' and '1.0' are the
        same DECIMAL; '2020-01-01' and ' 2020-01-01' the same DATE; '10:00:00' and '10:00:00.0', or
        '2020-01-01' and '2020-01-01 00:00:00', the same TIMESTAMP. The distinct keys are counted over the full
        table with the texts and with the converted values (the MODIFY would fail, or leave duplicate keys under
        a disabled PRIMARY KEY).
      - Column names: a column whose name contains the token TS, DT, DATE, TIMESTAMP, DATETIME or DOB
        (separated by '_', e.g. ORDER_DATE, but not UPDATED or PRODUCTS) and whose values do not parse gets a
        hint in "notes"; its VARCHAR shrink suggestion is kept.
      Not converted (Keep + note):
      - two-digit years such as '12.06.26', also in the session formats (YYYY would read '26' as year 0026,
        YY / RR pick a century implicitly; the century must be decided by you; the note names the two-digit
        forms the values fit, e.g. 'DD.MM.YY');
      - day/month orders that are ambiguous (every day <= 12, or more than one explicit format fits);
      - timestamps with a time zone ('Z' or +hh:mm), because a conversion would drop the zone;
      - calendar months 'YYYY-MM' (not durations; a DATE can be built with TO_DATE(col, 'YYYY-MM'));
      - DOUBLE candidates with more than 15 significant digits, a leading zero or a '+' sign;
      - numbers that need more than 36 digits, intervals or timestamps with more than 9 digits of precision,
        and day-to-second intervals with more than 3 fractional-second digits (the type keeps milliseconds).
      Values that do not all fit one type get the VARCHAR shrink (+ note) instead of a conversion: text mixed
      with numbers or dates, intervals mixed with calendar months 'YYYY-MM', timestamps with and without a
      time zone, plain numbers mixed with DOUBLE candidates that would lose digits, and dates mixed with
      timestamps when a date does not parse with the target TIMESTAMP format.

    !!! REVIEW BEFORE YOU APPLY ANYTHING - a conversion can be LOSSY !!!
      - '007' -> 7 and '+49' -> 49: leading zeros and signs are lost (WARNING in notes; zip codes, phone and
        article numbers are typical traps).
      - '1.10' -> 1.1: the text form (e.g. of version numbers) is lost (note).
      - ' 5 ' -> 5, '5.' -> 5, '.5' -> 0.5, '-0' -> 0: blank padding (also of dates and timestamps) and these
        number forms are lost (note).
      - 'Y', 'yes', 'T' and '1' all become TRUE (note).
      - DOUBLE PRECISION keeps about 15 significant digits (WARNING).
      - Integers that look like dates (YYYYMMDD) stay numbers (the note suggests TO_DATE(col, 'YYYYMMDD')).
      - Values that are loaded LATER must fit the new type as well; a DECIMAL rounds extra decimals silently.

    OUTPUT (all six columns are VARCHAR(2000000))
      1. A header row: the NLS settings and the sample method of this analysis.
      2. Note rows, only when relevant: FOREIGN KEYs read from SYS.EXA_ALL_CONSTRAINTS (restricted user), or
         an error when the FOREIGN KEY catalog could not be read (then every column row says "Not changed: ...
         (suggested type ...)" and has no statement).
      3. One row per column, sorted by schema, table, column: schema_name, table_name, column_name,
         conversion (e.g. "VARCHAR(100) UTF8 --> DECIMAL(9, 0)" or "Keep VARCHAR(100) UTF8, max length: 12"),
         query_text (the statement(s); empty for Keep rows) and notes. Other row texts: "Table is empty (no
         data)", "Could not analyze ...", "Keep ... (FK key column - related table out of scope)". In a
         sampled run the notes also say whether the table was sampled or read completely (and why).
      4. A "COMMIT;" row after these column changes (only when there is at least one).
      5. FOREIGN KEY key columns that are NOT changed (their key group reaches a table outside the filter, or
         the key columns have no common type), with the reason, under -- ### FOREIGN KEY NOTES (not changed) ###.
      6. When FOREIGN KEYs are involved: the FOREIGN KEY block (see below), which ends with its own COMMIT row.
      7. A last row that restores the session NLS settings (only when a statement changes them). It ends with
         COMMIT, because in Exasol ROLLBACK also reverts ALTER SESSION (see RUNNING THE STATEMENTS).
      Section dividers are rows with empty names whose query_text is an SQL comment ("-- ### ... ###").

    RUNNING THE STATEMENTS
      - Each query_text cell is self-contained: a statement whose result depends on the session NLS settings
        (DATE, TIMESTAMP, DECIMAL with decimals, DOUBLE) starts with ALTER SESSION statements that set them
        exactly as they were during the analysis, or as the detected explicit format requires. Run a cell as
        a whole. The column cells do not depend on each other's order; only the FOREIGN KEY block has a fixed
        order (DROP first, key column changes, RE-ADD last) and the COMMIT rows end a transaction. The last
        row restores the NLS settings that were active when the analysis started and ends with COMMIT.
      - A column DEFAULT is treated like one more value. A literal DEFAULT of a column that becomes DATE,
        TIMESTAMP, DECIMAL or DOUBLE is re-set as a typed literal after the change (ALTER COLUMN ... SET
        DEFAULT), so new rows do not depend on the NLS settings of the inserting session. Only a quoted text
        or an integer counts as a literal DEFAULT: a column with any other DEFAULT (e.g. CURRENT_USER, or an
        unquoted decimal number such as 1.5) is kept.
      - Run the statements with AUTOCOMMIT OFF. The column changes and the FOREIGN KEY block are two separate
        transactions, each ended by its COMMIT row. After any error run ROLLBACK: it reverts only the
        statements since the last COMMIT (inside the FOREIGN KEY block it also restores the dropped FOREIGN
        KEYs and the key columns). In Exasol ROLLBACK also reverts ALTER SESSION: the session falls back to
        the NLS settings of the last cell before the last COMMIT (e.g. an explicit 'MM.DD.YYYY'). After a
        ROLLBACK therefore run the NLS restore row (the last row) again; it ends with COMMIT, so a later
        ROLLBACK cannot undo it.

    FOREIGN KEY HANDLING
      In Exasol a FOREIGN KEY column and the referenced PRIMARY KEY column must have the same type, so they
      can only be changed together. For every referential key group whose tables are all inside the filter,
      ONE common target type that fits all of its columns is computed (a group is never harmonized to DOUBLE
      PRECISION or to more than 36 digits; a DATE member of a TIMESTAMP group is verified) and emitted in a
      FOREIGN KEY block:
        -- ### FOREIGN KEY BLOCK - run the following rows as ONE transaction (AUTOCOMMIT OFF, ROLLBACK on any error) ###
        -- ### DROP FOREIGN KEYS - run these FIRST (before the key column changes) ###
        -- ### KEY COLUMN TYPE CHANGES ###
        -- ### RE-ADD FOREIGN KEYS - run these LAST (after the key column changes) ###
        COMMIT;
      When the block exists, the other column changes are listed before it under
        -- ### COLUMN TYPE CHANGES (no FOREIGN KEY involved) ###
      and stay outside the block. Key columns that are not changed are listed (without a statement) under
        -- ### FOREIGN KEY NOTES (not changed) ###
      Every FOREIGN KEY is re-added in its original ENABLE / DISABLE state. A key column whose group reaches
      a table outside the filter is kept, with a note that names the other table(s): re-run with a
      schema_pattern / table_pattern that includes them.
      A key column without data (its table is empty, or the column is NULL in every row, and it has no
      DEFAULT) fits every type: it does not take part in finding the common type and gets the type the other
      members agree on (its MODIFY is in the FOREIGN KEY block); when no member of a group holds data, the
      group is kept; a member without data but with a DEFAULT does not get this treatment (its DEFAULT is not
      checked against the common type), so its group is kept. A key column with only 0/1 values (BOOLEAN on
      its own) is merged as a number, so a key group is never BOOLEAN next to a numeric member; only together
      with a text boolean member (Y/N, TRUE/FALSE, ...) does it stay BOOLEAN. When a PRIMARY KEY of the group
      would get duplicate keys after the conversion (BOOLEAN, DECIMAL, DATE, TIMESTAMP, ...), the group is kept.
      FOREIGN KEYs are read from SYS.EXA_DBA_CONSTRAINTS when the user may read it, otherwise from
      SYS.EXA_ALL_CONSTRAINTS; then FOREIGN KEYs on tables the user cannot see are unknown, and a note row
      says so. The catalog is queried once per call. If it cannot be read at all, an error row is shown and
      no statement is emitted for any column, because the key columns are unknown (the rows still show the
      suggested type). A key group whose columns are all kept for their own reasons is not reported as
      blocked.

    PRIVILEGES
      SELECT on the analyzed tables (otherwise "Could not analyze ..."); to run the statements: ALTER on the
      tables, and REFERENCES for re-added FOREIGN KEYs.

    PARAMETERS
      schema_pattern      : schema name or LIKE pattern, case-exact (% = any characters, _ = any single
                            character). A backslash escapes a following %, _ or backslash (MY\_SCHEMA matches
                            only MY_SCHEMA); any other backslash is a normal character. This does not depend
                            on the session parameter DEFAULT_LIKE_ESCAPE_CHARACTER.
      table_pattern       : table name or LIKE pattern, same rules.
      sample_size         : number of rows per table (an integer; values below 1000 are raised to 1000), or a
                            percentage string such as '5%' (greater than 0% and at most 100%; at least 1000 rows
                            per table). '100%' reads every row. Tables with at most 500,000 rows are read
                            completely when the requested sample covers at least 5% of them ('5%' or more, or a
                            number of rows of at least 5% of the table; on the reference cluster a full scan was
                            not slower than a 5% sample at 250,000 and 500,000 rows).
                            Any other value gives an error row.
      log_for_all_columns : true  = report every inspected column, incl. "Keep ..." rows with the reason;
                            false = report the columns that get a statement, plus every "Could not analyze ..."
                            row and every FOREIGN KEY note (key groups blocked from a conversion, key columns
                            whose group reaches outside the filter). Only the boolean true switches it on
                            (NULL = false).
      An empty table is listed as "Table is empty (no data)" and an empty column as a DROP COLUMN candidate
      (both only with log_for_all_columns = true).

    VARCHAR shrink width: the longest value + 20%, rounded UP to the next multiple of its leading power of
    ten (1 -> 3, 8 -> 20, 12 -> 20, 500 -> 700, 1000 -> 2000), at most 2,000,000. Columns with a width of 3 or
    less are not shrunk.

    No Animals Were Harmed in the Making of This Script.
*/

--parameter	schema_pattern:      schema name or LIKE pattern (case-exact; % and _ are wildcards; a backslash escapes % / _ / backslash)
--parameter	table_pattern:       table name or LIKE pattern (same rules)
--parameter	sample_size:         number of rows per table (min 1000) or a percentage string like '5%' ('100%' = every row; tables up to 500,000 rows are read completely when the sample covers at least 5% of them)
--parameter	log_for_all_columns: false = columns that get a statement plus error and FOREIGN KEY rows, true = every inspected column
--/
CREATE OR REPLACE SCRIPT database_migration.convert_varchar(schema_pattern, table_pattern, sample_size, log_for_all_columns) RETURNS TABLE AS

    ------------------------------------------------------------------------------------------------------
    -- DbVisualizer note: this file must never contain the question-mark character, a colon directly
    -- followed by a quote, or a backslash directly before a quote (DbVisualizer would ask for parameters or
    -- lose track of string literals at install time). Therefore the backslash is produced with
    -- string.char(92), the regex zero-or-one quantifier is written as {0,1}, and a colon inside SQL text is
    -- written as CHR(58) or passed in a bind value.
    ------------------------------------------------------------------------------------------------------

    local OPT = { geo_gate = true, probe_distinct = true, dedup = true, witness = true, probe_pilot = true, bool_gate = true, len_gate = true, batch_counts = true, batch_verify = false, auto_full_scan = true, opt_min_sample = 20000, pilot_min_sample = 100000, full_scan_max_rows = 500000 }

    ------------------------------------------------------------------------------------------------------
    -- PERFORMANCE OPTIONS (the line above). Every option only changes HOW the facts are computed, never
    -- their values: with every flag set to false the script is the unoptimized reference, and both builds
    -- return the same conversion and query_text (auto_full_scan changes only the notes and the header
    -- of a sampled run; with '100%' the output is identical). The options of EVERY statement follow the
    -- rows that statement reads: a statement over the random sample uses the options for the sample size,
    -- a statement over the full column (verification, full classification, full probe, the hints and the
    -- batched pass) the options for the table row count. A statement that reads fewer than
    -- opt_min_sample rows runs exactly like the reference (there the extra statements cost more than they
    -- save); the probe pilot needs pilot_min_sample rows.
    --   geo_gate       the GEOMETRY regex runs only for values that contain '(' or EMPTY and a WKT keyword
    --                  (every WKT value the regex accepts contains both, so the class is unchanged).
    --   len_gate       a value with more than LEN_BOUND non-blank characters that contains a character
    --                  other than blanks, '+', '-', '.', the colon and digits skips the DATE / TIMESTAMP /
    --                  interval / 'YYYY-MM' checks (it cannot pass any of them, see cat_sql).
    --   bool_gate      IS_BOOLEAN is evaluated only for the value classes that can be boolean (see aggs).
    --   dedup          a column with few distinct values (APPROXIMATE_COUNT_DISTINCT <= 30% of the analysed
    --                  rows) is classified once per DISTINCT value, every count weighted with the number of
    --                  rows of the value.
    --   witness        a value of the first 1000 rows that is text (class OTH), not boolean and not a
    --                  timestamp with a time zone proves "no single type" for every row set that contains it;
    --                  the classification then computes only the facts that this case reads, and checks in
    --                  the same statement that the value is really in the set.
    --   probe_distinct the explicit-format probe runs over the DISTINCT values (weighted with their row
    --                  count) and parses only the DATE formats or only the TIMESTAMP formats (the one set
    --                  its decision reads).
    --   probe_pilot    a probe format that fails on one of the first <= 1000 distinct values is not
    --                  evaluated on the other values (its count is only compared with "every value").
    --   batch_counts   a sampled table computes the full-column data count and MAX(LENGTH) of up to 20
    --                  columns in ONE statement instead of one statement per column (see batch_full_facts).
    --   batch_verify   the batched statement also computes the verification facts of every column with a
    --                  proposal (instead of one verification statement per column). The aggregates, the
    --                  classification and the DEFAULT row are the ones of the per-column statements; a failing
    --                  batched statement falls back to the per-column statements. OFF: measured slower on a
    --                  4-node cluster (60 columns; '5%' / '1%': 1M rows 17.2 / 14.4 s instead of 11.8 / 9.1 s,
    --                  10M rows 108 / 98 s instead of 42 / 32 s), because the batched rows cannot be grouped by
    --                  value (dedup) and every row is classified for 20 columns; batch_counts alone saves time
    --                  (1M: 0.35 s instead of 0.79 s, 10M: 3.0 s instead of 3.7 s for the 60 counts).
    --   auto_full_scan a table with at most full_scan_max_rows rows is read completely even when a sample is
    --                  requested, but only when the requested sample covers at least AUTO_MIN_PCT (5) percent of
    --                  the table: a percentage of at least 5%, or a number of rows of at least 5% of COUNT(*) (for
    --                  a number of rows n the limit is therefore min(full_scan_max_rows, 20 * n)); a smaller
    --                  sample keeps the sampled path. A full scan gives the same conversion and query_text as the
    --                  sampled run (whose proposals are verified against the full column anyway) with fewer
    --                  statements. Measured on a 4-node cluster (60 VARCHAR columns, one run per point, full scan
    --                  time minus sample time): 250,000 rows '5%' -0.61 s, '3%' -0.27 s, '2%' +0.52 s, '1%' +1.13 s;
    --                  500,000 rows '5%' -0.62 s, '4%' -0.02 s, '3%' +0.82 s, 10000 rows +1.72 s, '1%' +2.38 s;
    --                  750,000 rows '5%' +0.67 s; 1M rows '5%' +1.53 s. '5%' and '100%' cross at about 620,000
    --                  rows (straight line between 500,000 and 750,000); the limit stays below it. '1%' (and 10000
    --                  rows of 500,000) was materially faster than a full scan below the limit, from 5% the full
    --                  scan was faster at both measured sizes: hence the 5% rule. It depends on the hardware and the data; set
    --                  full_scan_max_rows = 0 to always sample as requested.
    -- Every optimized statement that fails is repeated as the reference statement.
    ------------------------------------------------------------------------------------------------------

    local BS         = string.char(92)       -- backslash
    local SAMPLE_MIN = 1000                  -- minimum sample rows per table (also for percentages)
    local SAMPLE_PRINT_MAX = 1e15            -- a sample_size of this many rows or more is printed as 'at least 10^15'
    local AUTO_MIN_PCT = 5                   -- auto_full_scan only for a sample of at least this percentage (see OPT)
    local OUT_COLS   = "schema_name VARCHAR(2000000), table_name VARCHAR(2000000), column_name VARCHAR(2000000), "
                    .. "conversion VARCHAR(2000000), query_text VARCHAR(2000000), notes VARCHAR(2000000)"

    ------------------------------------------------------------------------------------------------------
    -- Small helpers
    ------------------------------------------------------------------------------------------------------
    -- SQL NULL arrives as null (not nil) in Exasol Lua.
    local function isnull(v) return v == nil or v == null end

    -- A query value (Lua number or Exasol decimal) as a Lua number; nil for NULL.
    local function num(v)
        if isnull(v) then return nil end
        return tonumber(tostring(v))
    end

    -- Like num(), but NULL becomes 0 (a MAX/SUM over no matching row is NULL).
    local function nz(v) return num(v) or 0 end

    local function trim(s) return (string.gsub(string.gsub(s, '^%s+', ''), '%s+$', '')) end

    -- A string as an SQL string literal (single quotes doubled).
    local function sql_str(s) return "'" .. (string.gsub(s, "'", "''")) .. "'" end

    -- Ends the script and returns the rows.
    local function finish(rows) exit(rows, OUT_COLS) end
    local function fail(msg) finish({ {'', '', '', msg, '', ''} }) end

    -- LIKE pattern for ESCAPE CHR(92): a backslash before %, _ or a backslash stays an escape; any other
    -- backslash (also a trailing one) is doubled, i.e. taken literally (a lone escape would be an error).
    local function like_value(v)
        local out, i, n = {}, 1, #v
        while i <= n do
            local ch = string.sub(v, i, i)
            if ch == BS then
                local nx = string.sub(v, i + 1, i + 1)
                if nx == '%' or nx == '_' or nx == BS then
                    out[#out + 1] = BS .. nx; i = i + 2
                else
                    out[#out + 1] = BS .. BS; i = i + 1
                end
            else
                out[#out + 1] = ch; i = i + 1
            end
        end
        return table.concat(out)
    end

    -- Only the binds a statement really references (pquery gets no unused parameters).
    local COLON = string.char(58)
    local function pick_binds(sql, all)
        local b = {}
        for k, v in pairs(all) do
            if string.find(sql, COLON .. k, 1, true) ~= nil then b[k] = v end
        end
        return b
    end

    -- DECIMAL storage classes: precision 9 (32 bit), 18 (64 bit) or 36 (128 bit).
    local function adjust_precision(prec)
        if prec <= 9 then return 9 elseif prec <= 18 then return 18 else return 36 end
    end

    -- VARCHAR shrink width: longest value + 20%, rounded UP to the next multiple of its leading power of ten
    -- (1 -> 3, 8 -> 20, 500 -> 700, 1000 -> 2000), at most 2,000,000.
    local function estimate_optimal_varchar_length(n)
        local n_p20            = math.ceil(n + n * 0.2)
        local number_digits    = string.len(string.format('%d', n_p20))
        local magnitude        = math.floor(10 ^ (number_digits - 1))
        local estimated_length = math.floor(n_p20 / magnitude) * magnitude + magnitude
        return math.min(estimated_length, 2000000)
    end

    -- One session parameter, nil when it cannot be read.
    local function session_param(name)
        local ok, r = pquery([[SELECT session_value FROM SYS.EXA_PARAMETERS WHERE parameter_name = :p]], {p = name})
        if ok and #r == 1 and not isnull(r[1][1]) then return r[1][1] end
        return nil
    end

    local function by_name(x, y)
        if x.sch ~= y.sch then return x.sch < y.sch end
        if x.tab ~= y.tab then return x.tab < y.tab end
        return x.col < y.col
    end

    ------------------------------------------------------------------------------------------------------
    -- Parameters
    ------------------------------------------------------------------------------------------------------
    if type(schema_pattern) ~= 'string' or schema_pattern == '' or
       type(table_pattern)  ~= 'string' or table_pattern  == '' then
        fail('Invalid parameters: schema_pattern and table_pattern must be non-empty strings.')
    end
    local log_all = (log_for_all_columns == true)   -- NULL and the string 'false' are NOT true
    local schp    = like_value(schema_pattern)
    local tabp    = like_value(table_pattern)

    -- sample_size: rows (min 1000) or a percentage string ('5%', ' 50 %', '2.5%'); anything else is an error
    local sample_rows, sample_pct, sample_ok = 0, 0, false
    local ss = sample_size
    if type(ss) == 'userdata' and not isnull(ss) then ss = num(ss) end
    if type(ss) == 'number' then
        if ss >= 1 then sample_rows = math.max(math.floor(ss), SAMPLE_MIN); sample_ok = true end
    elseif type(ss) == 'string' then
        local s = trim(ss)
        local p = string.match(s, '^(%d+)%s*%%$') or string.match(s, '^(%d*%.%d+)%s*%%$')
        if p ~= nil then
            sample_pct = tonumber(p)
            sample_ok  = (sample_pct ~= nil and sample_pct > 0 and sample_pct <= 100)
        elseif string.match(s, '^%d+$') ~= nil and tonumber(s) >= 1 then
            sample_rows = math.max(tonumber(s), SAMPLE_MIN); sample_ok = true
        end
    end
    if not sample_ok then
        fail("Invalid sample_size: use a number of rows (at least 1, values below 1000 are raised to 1000) or a percentage string greater than 0% and at most 100%, e.g. '5%' or '100%'.")
    end
    local sampled_run = (sample_rows > 0) or (sample_pct < 100)      -- false only for '100%'

    ------------------------------------------------------------------------------------------------------
    -- Session NLS settings at detection time. Every NLS-dependent statement in the output sets them itself
    -- and the last output row restores them.
    ------------------------------------------------------------------------------------------------------
    local nls_date_format      = session_param('NLS_DATE_FORMAT')
    local nls_timestamp_format = session_param('NLS_TIMESTAMP_FORMAT')
    local nls_numeric          = session_param('NLS_NUMERIC_CHARACTERS')
    if nls_date_format == nil or nls_timestamp_format == nil or nls_numeric == nil or nls_numeric == '' then
        fail('Could not read the session NLS settings (NLS_DATE_FORMAT, NLS_TIMESTAMP_FORMAT, NLS_NUMERIC_CHARACTERS) from SYS.EXA_PARAMETERS.')
    end
    local dec_char = string.sub(nls_numeric, 1, 1)
    -- month / day names (MON, MONTH, DAY, DY) in a format also depend on NLS_DATE_LANGUAGE
    local nls_date_language = session_param('NLS_DATE_LANGUAGE')
    local function uses_names(fmt) return string.find(fmt, 'MON', 1, true) ~= nil or string.find(fmt, 'DY', 1, true) ~= nil or string.find(fmt, 'DAY', 1, true) ~= nil end

    -- Fractional-second digits the session NLS_TIMESTAMP_FORMAT parses (FF<n>; 'FF' alone means 6).
    local nls_ff = 0
    local ff_digit = string.match(nls_timestamp_format, 'FF(%d)')
    if ff_digit ~= nil then
        nls_ff = tonumber(ff_digit)
    elseif string.find(nls_timestamp_format, 'FF', 1, true) ~= nil then
        nls_ff = 6
    end

    -- Session TIMESTAMP format for values with fp fractional digits: the detection format itself, or - when it
    -- parses fewer digits - the same format with FF<fp>, so the conversion does not truncate the fraction.
    local function session_ts_format(fp)
        if fp <= nls_ff then return nls_timestamp_format end
        local f, n = string.gsub(nls_timestamp_format, 'FF%d*', 'FF' .. fp)
        if n == 0 then f = nls_timestamp_format .. '.FF' .. fp end
        return f
    end

    -- Day/month-swapped sibling of a format that starts with DD and MM (e.g. DD.MM.YYYY -> MM.DD.YYYY);
    -- nil for other formats (e.g. YYYY-MM-DD, where nobody writes YYYY-DD-MM).
    local function swapped_format(fmt)
        local sep, rest = string.match(fmt, '^DD([^%w])MM(.*)$')
        if sep ~= nil then return 'MM' .. sep .. 'DD' .. rest end
        sep, rest = string.match(fmt, '^MM([^%w])DD(.*)$')
        if sep ~= nil then return 'DD' .. sep .. 'MM' .. rest end
        return nil
    end
    local date_swap = swapped_format(nls_date_format)
    local ts_swap   = swapped_format(nls_timestamp_format)

    -- Digit groups of a date written in the session NLS_DATE_FORMAT (3 for 'MM/DD/YYYY', 2 for 'DD-MON-YYYY', 1 for
    -- 'YYYYMMDD'; the same for every date, a format element always renders digits or letters). IS_DATE also accepts a
    -- trailing time in some formats (measured on Exasol 2025.1: '01/02/2000 10:00:01' and '01/02/2000 10' under
    -- 'MM/DD/YYYY'), and a DATE keeps no time of day: a value with MORE digit groups than a date has a time part
    -- (r_dtime) and is never classified DATE (see cat_sql).
    local okg, rg = pquery([[SELECT TO_CHAR(DATE '2000-01-02', :f) FROM SYS.DUAL]], { f = nls_date_format })
    if not okg or #rg ~= 1 or isnull(rg[1][1]) then
        fail('Could not render a date with the session NLS_DATE_FORMAT (needed to tell dates from timestamps).')
    end
    local _, date_groups = string.gsub(rg[1][1], '%d+', '')
    -- An NLS_DATE_FORMAT can contain time elements (e.g. 'MM/DD/YYYY HH12:MI:SS AM' or 'DD.MM.YYYY HH24:MI'). A date
    -- then renders with a time ('01/02/2000 12:00:00 AM'), a value with a time has no more digit groups than that,
    -- r_dtime cannot see the time, and TO_DATE drops it. date_fmt_time = a time of day changes the rendering of the
    -- format; then a value is 'DATE' only when the date format also parses it as a TIMESTAMP at midnight (cat_sql,
    -- default_statement). For a format without time elements nothing changes.
    local okt, rt = pquery([[SELECT CASE WHEN TO_CHAR(TIMESTAMP '2000-01-02 13:14:15.123456', :f) = TO_CHAR(DATE '2000-01-02', :f) THEN 0 ELSE 1 END FROM SYS.DUAL]],
                           { f = nls_date_format })
    if not okt or #rt ~= 1 or isnull(rt[1][1]) then
        fail('Could not render a timestamp with the session NLS_DATE_FORMAT (needed to tell dates from timestamps).')
    end
    local date_fmt_time = (tonumber(rt[1][1]) == 1)

    ------------------------------------------------------------------------------------------------------
    -- Regular expressions (passed as bind values; REGEXP_LIKE matches the whole string).
    ------------------------------------------------------------------------------------------------------
    local dec_re = BS .. dec_char             -- escaped decimal separator
    -- Every pattern starts with '^'. REGEXP_LIKE matches the whole string anyway, so '^' changes no result; it only
    -- tells the regex engine that a match can start at the first character. Without it a pattern that starts with
    -- ' *' is tried again at every position of a long run of blanks (time grows with the square of the length:
    -- measured on Exasol 2025.1, r_geo took 0.2 s for 20,000 blanks + 'x' and 0.8 s for 40,000, so a value of
    -- 2,000,000 characters needed far more than 10 minutes); with '^' each pattern is linear (0.02 s at 40,000).
    local RX = {
        r_int     = '^ *[+-]{0,1}[0-9]+ *$',
        r_dec     = '^ *[+-]{0,1}([0-9]+' .. dec_re .. '[0-9]*|' .. dec_re .. '[0-9]+) *$',          -- '1.5', '.5', '5.'
        r_dpr     = '^ *[+-]{0,1}([0-9]+(' .. dec_re .. '[0-9]*){0,1}|' .. dec_re .. '[0-9]+)[eE][+-]{0,1}[0-9]+ *$',
        r_lead0   = '^ *[+-]{0,1}0[0-9].*',                                      -- leading zero: '007', '-01'
        r_plus    = '^ *[+].*',                                                  -- leading '+': '+49170'
        r_tzero   = '^.*' .. dec_re .. '[0-9]*0 *',                              -- trailing zero decimal: '1.10'
        r_exp     = '[eE].*$',
        r_nondig  = '[^0-9]',
        r_period  = '^ *[0-9]{4}-[0-9]{2} *',                                    -- calendar month 'YYYY-MM'
        r_geo     = '^ *(MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION|LINESTRING|LINEARRING|POLYGON|POINT) *([(].*[)]|EMPTY) *',
        r_frac    = '[0-9]:[0-9]{1,2}[.,][0-9]+',                               -- fraction AFTER the seconds
        r_frachd  = '^[0-9]:[0-9]{1,2}[.,]',
        r_zone    = '^ *[0-9].*[0-9]:[0-9]{2}(:[0-9]{2}([.,][0-9]+){0,1}){0,1} *(Z|UTC|GMT|[+-][0-9]{2}(:{0,1}[0-9]{2}){0,1}) *',
        r_dateish = '^ *[0-9][0-9 .,:/T-]*[0-9] *$',
        r_y4      = '^ *([0-9]{4}[^0-9][0-9]{1,2}[^0-9][0-9]{1,2}|[0-9]{1,2}[^0-9][0-9]{1,2}[^0-9][0-9]{4})([^0-9].*){0,1}',
        r_lgate   = '^[0-9 +.:-]*',                                              -- the only characters an interval can have
        -- a time part: more digit groups than a date in the session NLS_DATE_FORMAT (r_dtime), or than the dates of
        -- the explicit probe formats, which all have 3 (r_ptime); every character is a digit or a non-digit, so the
        -- patterns also hold for line breaks
        r_dtime   = '^[^0-9]*([0-9]+[^0-9]+){' .. date_groups .. '}[0-9]+([^0-9]+[0-9]+)*[^0-9]*',
        r_ptime   = '^[^0-9]*([0-9]+[^0-9]+){3}[0-9]+([^0-9]+[0-9]+)*[^0-9]*',
        -- midnight in an explicit probe format: every digit group after the 3 of the date is all zeros (HH24, MI,
        -- SS and the fraction; a date without a time matches as well), e.g. '2000-01-02 00:00:00.000'
        r_pmid    = '^[^0-9]*([0-9]+[^0-9]+){2}[0-9]+([^0-9]+0+)*[^0-9]*',
    }
    RX.ndf = nls_date_format     -- not a pattern: the session date format, bound with the patterns (cat_sql)

    -- len_gate bound (see cat_sql): an accepted DATE / TIMESTAMP text has at most 10 non-blank characters per
    -- character of the session formats (the longest rendering of a format element, e.g. DAY -> 'WEDNESDAY');
    -- 100 characters margin.
    local LEN_BOUND = 100 + 10 * (string.len(nls_date_format) + string.len(nls_timestamp_format))

    ------------------------------------------------------------------------------------------------------
    -- SQL building blocks. The inner query always exposes: col (the value), src_row (1 = a table row,
    -- 0 = the column DEFAULT, treated as one more value) and cat (the class of the value).
    ------------------------------------------------------------------------------------------------------
    -- Each building block is a function of the value expression x (col in the per-column statements, a
    -- column alias in the batched statement, see batch_full_facts); SQL_TIME_FRAC is the col form (probe).
    local function uns_of(x) return "LTRIM(TRIM(" .. x .. "), '+-')" end        -- value without blanks and sign
    -- integer digits without leading zeros ('007' -> 1, '.5' -> NULL = 0); NULL-safe via nz() in Lua
    local function int_digits_of(x)
        local u = uns_of(x)
        return "LENGTH(LTRIM(CASE WHEN INSTR(" .. u .. ", :dec) > 0 THEN SUBSTR(" .. u .. ", 1, INSTR(" .. u .. ", :dec) - 1) ELSE " .. u .. " END, '0'))"
    end
    local function frac_digits_of(x) local u = uns_of(x) return "LENGTH(RTRIM(SUBSTR(" .. u .. ", INSTR(" .. u .. ", :dec) + 1)))" end
    local function sig_digits_of(x) return "LENGTH(LTRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(" .. x .. "), :r_exp), :r_nondig), '0'))" end
    -- fractional-second digits, measured AFTER hh:mi:ss (never the '.' of a DD.MM.YYYY date), '.' or ','
    local function time_frac_of(x) return "LENGTH(REGEXP_REPLACE(REGEXP_SUBSTR(" .. x .. ", :r_frac), :r_frachd))" end
    local SQL_TIME_FRAC   = time_frac_of('col')

    -- NOOPT: no optimization (the reference statements); a table gets its own options in the main loop.
    local NOOPT = { any = false }

    -- The class of one value x (col, or a column alias). One classification for the sample AND for every
    -- full-column verification, so a verified proposal is exactly the proposal a full scan would make.
    -- Dates and timestamps: a DATE keeps no time of day, so a value with a time part (r_dtime: more digit groups
    -- than a date in NLS_DATE_FORMAT, e.g. ' 10:00:01', ' 00:00:00' or an hour alone) is never 'DATE'. It is 'TS'
    -- when IS_TIMESTAMP accepts it (also at midnight); otherwise it is not a date or timestamp of the session
    -- formats (e.g. '01/02/2000 10:00:01' under NLS_TIMESTAMP_FORMAT 'MM/DD/YYYY HH12:MI:SS AM', where AM/PM is
    -- missing, although IS_DATE accepts it under 'MM/DD/YYYY'), and the explicit-format probe decides. A value
    -- without a time part is 'TS' only when IS_TIMESTAMP gives a time of day other than midnight. When
    -- NLS_DATE_FORMAT itself contains time elements (date_fmt_time), a value is 'DATE' only when that format also
    -- parses it as a TIMESTAMP at midnight ('01/13/2000 12:00:00 AM' or '01/13/2000' under 'MM/DD/YYYY HH12:MI:SS
    -- AM', not '01/13/2000 10:00:01 PM'): every other time of day would be lost. The class never changes for a
    -- midnight time: a column whose 'TS' values are all at midnight (n_ts_nonmid = 0) gets DATE in finalize, and the
    -- statement reads it with the TIMESTAMP format before the change to DATE (render_change).
    -- o = the optimizations active for the table. Both gates only skip checks whose result is already known:
    --   geo_gate: the anchored GEOMETRY regex r_geo accepts only texts that contain a WKT keyword (each of the
    --     eight contains POINT, LINE, POLYGON or GEOMETRYCOLLECTION) AND '(' or EMPTY; any other value cannot
    --     match, so the regex is evaluated only behind that pre-check (NULL fails both -> 'OTH' in both).
    --   len_gate: evaluated after IS_NUMBER (which is not gated). A value with more than LEN_BOUND characters
    --     that are not blanks and with a character outside '0-9 +.:-' is sent to the GEOMETRY check / 'OTH'.
    --     Facts measured on Exasol 2025.1 (four NLS settings): IS_DSINTERVAL / IS_YMINTERVAL accept unlimited
    --     blanks, leading zeros and fraction digits, but no character other than blanks, '+', '-', '.',
    --     digits and the colon (every printable ASCII character, TAB, LF, CR, NBSP and Unicode blanks
    --     inserted at every position; interval words
    --     such as '5 DAY' or 'P5D' are rejected); 'YYYY-MM' (r_period) has only digits, '-' and blanks.
    --     IS_DATE / IS_TIMESTAMP accept unlimited blanks only (no repeated separators or extra zeros, at
    --     most 9 fraction digits); the longest non-blank text any format element accepts is 10 characters
    --     per format character (J -> 7 digits, FF -> 9, DAY -> 'WEDNESDAY', MONTH -> 'SEPTEMBER'), so an
    --     accepted text has at most 10 * (LENGTH(NLS_DATE_FORMAT) + LENGTH(NLS_TIMESTAMP_FORMAT)) non-blank
    --     characters (measured maximum: 44 for 'DAY DD MONTH YYYY HH12:MI:SS.FF9 AM'). A gated value therefore
    --     fails TS, DATE, YMP, DSI and YMI in the ungated CASE as well and gets the same class.
    local function cat_sql(o, x)
        x = x or 'col'
        local U = "UPPER(" .. x .. ")"
        local geo = "WHEN " .. U .. " REGEXP_LIKE :r_geo THEN 'GEO'"
        if o.geo_gate then
            geo = "WHEN (INSTR(" .. x .. ", '(') > 0 OR INSTR(" .. U .. ", 'EMPTY') > 0) AND (INSTR(" .. U .. ", 'POINT') > 0 OR INSTR("
               .. U .. ", 'LINE') > 0 OR INSTR(" .. U .. ", 'POLYGON') > 0 OR INSTR(" .. U .. ", 'GEOMETRYCOLLECTION') > 0) THEN CASE WHEN "
               .. U .. " REGEXP_LIKE :r_geo THEN 'GEO' ELSE 'OTH' END"
        end
        local dmid = ""
        if date_fmt_time then
            dmid = " AND CASE WHEN IS_TIMESTAMP(" .. x .. ", :ndf) THEN CASE WHEN TO_TIMESTAMP(" .. x .. ", :ndf) = TRUNC(TO_TIMESTAMP("
                .. x .. ", :ndf)) THEN 1 ELSE 0 END ELSE 0 END = 1"
        end
        local lg = ""
        if o.len_gate then
            lg = " WHEN LENGTH(" .. x .. ") > " .. LEN_BOUND .. " AND LENGTH(REPLACE(" .. x .. ", ' ', '')) > " .. LEN_BOUND
              .. " AND NOT (" .. x .. " REGEXP_LIKE :r_lgate) THEN CASE " .. geo .. " ELSE 'OTH' END"
        end
        return "CASE WHEN IS_NUMBER(" .. x .. ") THEN CASE WHEN " .. x .. " REGEXP_LIKE :r_int THEN 'INT' WHEN " .. x
            .. " REGEXP_LIKE :r_dec THEN 'DEC' WHEN " .. x .. " REGEXP_LIKE :r_dpr THEN 'DBL' ELSE 'OTH' END"
            .. lg
            .. " WHEN IS_TIMESTAMP(" .. x .. ") AND (TO_TIMESTAMP(" .. x .. ") <> TRUNC(TO_TIMESTAMP(" .. x .. ")) OR " .. x .. " REGEXP_LIKE :r_dtime) THEN 'TS'"
            .. " WHEN IS_DATE(" .. x .. ") AND NOT (" .. x .. " REGEXP_LIKE :r_dtime)" .. dmid .. " THEN 'DATE'"
            .. " WHEN " .. x .. " REGEXP_LIKE :r_period THEN 'YMP'"
            .. " WHEN IS_DSINTERVAL(" .. x .. ") THEN 'DSI'"
            .. " WHEN IS_YMINTERVAL(" .. x .. ") THEN 'YMI' "
            .. geo .. " ELSE 'OTH' END"
    end

    -- The fact aggregates over (col, src_row, cat[, cnt]). dd = dedup: the rows are grouped by value and every
    -- count is weighted with cnt (the rows of the value), so COUNT / SUM give the same numbers as over the
    -- rows (MAX is unaffected). bg = bool_gate: IS_BOOLEAN only where it can be TRUE. Facts measured on
    -- Exasol 2025.1 (exhaustive over A-Z up to 5 characters, A-Za-z up to 3, '0-9 +-.,eE' up to 4, all
    -- printable ASCII up to 2, plus 561 hand-picked texts; identical under ISO / German / US NLS): IS_BOOLEAN
    -- accepts exactly  blanks* (T|F|Y|N|YES|NO|TRUE|FALSE, any case) blanks*  and  blanks* 0* (0|1) blanks*.
    -- The digit form is an integer (class INT); the word form has no digit and no '(' (never INT/DEC/DBL,
    -- YMP or GEO), so its class is OTH or - under an unusual NLS format - TS/DATE/DSI/YMI with at most 5
    -- characters after TRIM. Every value skipped by the gate (DEC, DBL, YMP, GEO, or TS/DATE/DSI/YMI longer
    -- than 5) is therefore not boolean and counted 0 in both builds; NULL counts 0 in both.
    -- x / c: the value expression and the class column (col / cat in the per-column statements; a column alias
    -- and its class alias in the batched statement (batch_full_facts), which builds the SAME aggregates per column).
    local AGG_CACHE = {}
    local function aggs(dd, bg, x, c)
        x, c = x or 'col', c or 'cat'
        local key = (dd and 'd' or '-') .. (bg and 'b' or '-') .. string.char(1) .. x .. string.char(1) .. c
        if AGG_CACHE[key] ~= nil then return AGG_CACHE[key] end
        local one = dd and 'cnt' or '1'
        local u   = uns_of(x)
        local sig = sig_digits_of(x)
        local tfr = time_frac_of(x)
        local function n_if(cond) return "SUM(CASE WHEN " .. cond .. " THEN " .. one .. " ELSE 0 END)" end
        local function n_if2(cond, test) return "SUM(CASE WHEN " .. cond .. " THEN CASE WHEN " .. test .. " THEN " .. one .. " ELSE 0 END ELSE 0 END)" end
        -- n_swap: DATE/TIMESTAMP values that ALSO parse with the day/month-swapped session format
        local swap_parts = {}
        if date_swap ~= nil then swap_parts[#swap_parts + 1] = "WHEN " .. c .. " = 'DATE' THEN CASE WHEN IS_DATE(" .. x .. ", :dswap) THEN " .. one .. " ELSE 0 END" end
        if ts_swap   ~= nil then swap_parts[#swap_parts + 1] = "WHEN " .. c .. " = 'TS' THEN CASE WHEN IS_TIMESTAMP(" .. x .. ", :tswap) THEN " .. one .. " ELSE 0 END" end
        local sql_swap = '0'
        if #swap_parts > 0 then sql_swap = 'SUM(CASE ' .. table.concat(swap_parts, ' ') .. ' ELSE 0 END)' end
        local A = {
            entries        = dd and ("SUM(CASE WHEN " .. x .. " IS NOT NULL THEN cnt ELSE 0 END)") or ("COUNT(" .. x .. ")"),
            data_entries   = dd and ("SUM(CASE WHEN src_row = 1 AND " .. x .. " IS NOT NULL THEN cnt ELSE 0 END)") or ("COUNT(CASE WHEN src_row = 1 THEN " .. x .. " END)"),
            def_cat        = "MAX(CASE WHEN src_row = 0 THEN " .. c .. " END)",
            max_length     = "MAX(LENGTH(" .. x .. "))",
            n_int          = n_if(c .. " = 'INT'"),
            n_dec          = n_if(c .. " = 'DEC'"),
            n_dbl          = n_if(c .. " = 'DBL'"),
            n_date         = n_if(c .. " = 'DATE'"),
            n_ts           = n_if(c .. " = 'TS'"),
            n_dsi          = n_if(c .. " = 'DSI'"),
            n_ymi          = n_if(c .. " = 'YMI'"),
            n_period       = n_if(c .. " = 'YMP'"),
            n_geo          = n_if(c .. " = 'GEO'"),
            n_bool         = bg and n_if2(c .. " IN ('INT', 'OTH') OR (" .. c .. " IN ('TS', 'DATE', 'DSI', 'YMI') AND LENGTH(TRIM(" .. x .. ")) <= 5)", "IS_BOOLEAN(" .. x .. ")")
                                or n_if("IS_BOOLEAN(" .. x .. ")"),
            n_bool01       = n_if(x .. " IN ('0', '1')"),
            num_int_digits = "MAX(CASE WHEN " .. c .. " IN ('INT', 'DEC') THEN " .. int_digits_of(x) .. " END)",
            num_scale      = "MAX(CASE WHEN " .. c .. " = 'DEC' THEN " .. frac_digits_of(x) .. " END)",
            dbl_sig_digits = "MAX(CASE WHEN " .. c .. " = 'DBL' THEN " .. sig .. " END)",
            n_idlike       = n_if2(c .. " IN ('INT', 'DEC', 'DBL')", x .. " REGEXP_LIKE :r_lead0 OR " .. x .. " REGEXP_LIKE :r_plus"),
            n_tzero        = n_if2(c .. " = 'DEC'", x .. " REGEXP_LIKE :r_tzero"),
            n_yyyymmdd     = n_if2(c .. " = 'INT' AND LENGTH(TRIM(" .. x .. ")) = 8", "IS_DATE(TRIM(" .. x .. "), 'YYYYMMDD')"),
            n_underflow    = n_if2(c .. " = 'DBL'", "CAST(" .. x .. " AS DOUBLE) = 0 AND " .. sig .. " > 0"),
            ts_frac        = "MAX(CASE WHEN " .. c .. " = 'TS' THEN " .. tfr .. " END)",
            -- timestamps with a time of day other than midnight: the session format reads a time other than
            -- 00:00:00, or the fraction after hh:mi:ss has a digit other than 0 (measured on the text, so a fraction
            -- the session format would cut off still counts); 0 = every TS value is at midnight (see finalize)
            n_ts_nonmid    = n_if2(c .. " = 'TS'", "TO_TIMESTAMP(" .. x .. ") <> TRUNC(TO_TIMESTAMP(" .. x .. ")) OR LTRIM(REGEXP_REPLACE(REGEXP_SUBSTR("
                                   .. x .. ", :r_frac), :r_frachd), '0') IS NOT NULL"),
            -- two-digit years: the four-digit year the session format reads does not appear in the text (YYYY
            -- reads '26' as 0026, YY / RR / RRRR pick a century implicitly); independent of the separators
            n_short_year   = "SUM(CASE WHEN " .. c .. " = 'TS' THEN CASE WHEN INSTR(" .. x .. ", TO_CHAR(TO_TIMESTAMP(" .. x .. "), 'YYYY')) = 0 THEN " .. one .. " ELSE 0 END"
                          .. " WHEN " .. c .. " = 'DATE' THEN CASE WHEN INSTR(" .. x .. ", TO_CHAR(CAST(" .. x .. " AS DATE), 'YYYY')) = 0 THEN " .. one .. " ELSE 0 END ELSE 0 END)",
            n_swap         = sql_swap,
            dsi_p          = "MAX(CASE WHEN " .. c .. " = 'DSI' THEN LENGTH(SUBSTR(" .. u .. ", 1, INSTR(" .. u .. ", ' ') - 1)) END)",
            dsi_fp         = "MAX(CASE WHEN " .. c .. " = 'DSI' THEN " .. tfr .. " END)",
            ymi_p          = "MAX(CASE WHEN " .. c .. " = 'YMI' THEN LENGTH(SUBSTR(" .. u .. ", 1, INSTR(" .. u .. ", '-') - 1)) END)",
            n_zoned        = n_if2(c .. " = 'OTH'", x .. " REGEXP_LIKE :r_zone"),
            maybe_dateish  = "MAX(CASE WHEN " .. c .. " = 'OTH' THEN CASE WHEN " .. x .. " REGEXP_LIKE :r_dateish THEN 1 ELSE 0 END ELSE 0 END)",
        }
        AGG_CACHE[key] = A
        return A
    end

    -- Which facts each query computes. MAIN classifies (on the sample or the full column); the others verify
    -- one proposed family against the full column with the same classification and the SAME fact names, so
    -- one decision function serves both.
    local AGG_LIST = {
        MAIN  = {'entries', 'data_entries', 'def_cat', 'max_length', 'n_int', 'n_dec', 'n_dbl', 'num_int_digits',
                 'num_scale', 'dbl_sig_digits', 'n_idlike', 'n_tzero', 'n_yyyymmdd', 'n_underflow', 'n_bool',
                 'n_bool01', 'n_date', 'n_ts', 'ts_frac', 'n_short_year', 'n_swap', 'n_dsi', 'dsi_p', 'dsi_fp',
                 'n_period', 'n_ymi', 'ymi_p', 'n_geo', 'n_zoned', 'maybe_dateish', 'n_ts_nonmid'},
        NUM   = {'entries', 'def_cat', 'n_int', 'n_dec', 'n_dbl', 'num_int_digits', 'num_scale', 'dbl_sig_digits',
                 'n_idlike', 'n_tzero', 'n_yyyymmdd', 'n_underflow', 'n_bool01'},
        DT    = {'entries', 'def_cat', 'n_date', 'n_ts', 'ts_frac', 'n_short_year', 'n_swap', 'n_ts_nonmid'},
        BOOL  = {'entries', 'def_cat', 'n_bool'},
        DSI   = {'entries', 'def_cat', 'n_dsi', 'dsi_p', 'dsi_fp'},
        YMI   = {'entries', 'def_cat', 'n_ymi', 'n_period', 'ymi_p'},
        GEO   = {'entries', 'def_cat', 'n_geo'},
        ZONED = {'entries', 'def_cat', 'n_date', 'n_ts', 'n_zoned'},
    }
    -- proposed family -> verification query kind (every family the classification can propose, except OTHER)
    local VERIFY_KIND = { NUM = 'NUM', BOOL01 = 'NUM', DBL = 'NUM', DT = 'DT', BOOL = 'BOOL', DSI = 'DSI', YMI = 'YMI',
                          GEO = 'GEO', PERIOD = 'YMI', ZONED = 'ZONED' }

    ------------------------------------------------------------------------------------------------------
    -- Value sources
    ------------------------------------------------------------------------------------------------------
    -- The rows of one column: the table (a random sample when sampled = true) plus the literal column DEFAULT
    -- as one more value (src_row = 0). The DEFAULT is passed as a bind value, never as SQL text.
    local function source_sql(ctx, sampled)
        local s = [[SELECT ::col AS col, 1 AS src_row FROM ::sch.::tab]]
        if sampled then
            -- the threshold and the cap are formatted numbers (with '.'), so the SQL does not depend on the NLS
            s = s .. [[ WHERE RANDOM() < ]] .. ctx.p_lit .. [[ LIMIT ]] .. ctx.cap_lit
        end
        if ctx.defval ~= nil then
            s = [[SELECT col, src_row FROM (]] .. s .. [[) UNION ALL SELECT CAST(:defval AS VARCHAR(2000000)), 0 FROM SYS.DUAL]]
        end
        return s
    end

    local function base_binds(ctx)
        local b = { sch = quote(ctx.sch), tab = quote(ctx.tab), col = quote(ctx.col), defval = ctx.defval,
                    dec = dec_char, dswap = date_swap, tswap = ts_swap }
        for k, v in pairs(RX) do b[k] = v end
        return b
    end

    -- The options and the hint of ONE statement follow the rows that statement reads: a statement over the
    -- sample uses the options for the sample size (ctx.o) and the hints of the sample (ctx.hint); a statement
    -- over the full column uses the options for the table row count (ctx.of) and the hints over the full
    -- table (ctx.full_hint, computed on first use). On a full scan both are the same.
    local function stmt_options(ctx, sampled)
        if sampled then return ctx.o or NOOPT end
        return ctx.of or NOOPT
    end
    local function stmt_hint(ctx, sampled, o)
        if not (o.dedup or o.witness) then return nil end
        if sampled then return ctx.hint end
        if ctx.full_hint ~= nil then return ctx.full_hint(ctx.col) end
        return nil
    end

    -- The value source, grouped by value when dedup applies (cnt = rows of the value; h = the hint).
    local function use_dedup(h, o) return o.dedup == true and h ~= nil and h.dedup == true end
    local function grouped_source(ctx, sampled, dd)
        local src = source_sql(ctx, sampled)
        if dd then src = [[SELECT col, src_row, COUNT(*) cnt FROM (]] .. src .. [[) GROUP BY col, src_row]] end
        return src
    end

    -- The fact query of kind (MAIN or a verification kind) with the options o and the hint h.
    local function facts_sql(ctx, kind, sampled, o, h)
        local dd = use_dedup(h, o)
        local A = aggs(dd, o.bool_gate == true)
        local sel = {}
        for i, n in ipairs(AGG_LIST[kind]) do sel[i] = A[n] .. ' ' .. n end
        return [[SELECT ]] .. table.concat(sel, ', ') .. [[ FROM (SELECT col, src_row, ]] .. (dd and 'cnt, ' or '') .. cat_sql(o)
            .. [[ AS cat FROM (]] .. grouped_source(ctx, sampled, dd) .. [[))]]
    end

    -- witness option: the column has a witness value w (class OTH, not boolean, not a timestamp with a time
    -- zone; found in the first 1000 rows). Every row set that contains w has propose_family = OTHER (w fits
    -- no family: it is not INT/DEC/DBL/DATE/TS/YMP/DSI/YMI/GEO, not boolean, and not zoned), and the OTHER
    -- path reads only entries, data_entries, def_cat, max_length, maybe_dateish and n_zoned. This query
    -- computes exactly these (the class only for the values the two regexes select) and has_w; only when
    -- has_w = 1 (w is in THIS row set) are its facts used, else the full query runs. The other counters are
    -- set to -1, so no family condition (count = entries >= 1) can hold.
    local function witness_facts(ctx, sampled, o, h)
        local dd  = use_dedup(h, o)
        local A   = aggs(dd, o.bool_gate == true)
        local one = dd and 'cnt' or '1'
        local cat = cat_sql(o)
        local sql = [[SELECT ]] .. A.entries .. [[, ]] .. A.data_entries .. [[, MAX(CASE WHEN src_row = 0 THEN ]] .. cat .. [[ END), MAX(LENGTH(col)), ]]
                 .. [[MAX(CASE WHEN col REGEXP_LIKE :r_dateish THEN CASE WHEN ]] .. cat .. [[ = 'OTH' THEN 1 ELSE 0 END ELSE 0 END), ]]
                 .. [[SUM(CASE WHEN col REGEXP_LIKE :r_zone THEN CASE WHEN ]] .. cat .. [[ = 'OTH' THEN ]] .. one .. [[ ELSE 0 END ELSE 0 END), ]]
                 .. [[MAX(CASE WHEN col = :wit THEN 1 ELSE 0 END) FROM (]] .. grouped_source(ctx, sampled, dd) .. [[)]]
        local binds = base_binds(ctx)
        binds.wit = h.w
        local ok, r = pquery(sql, pick_binds(sql, binds))
        if not ok or nz(r[1][7]) ~= 1 then return false end
        local f = {}
        for _, n in ipairs(AGG_LIST.MAIN) do f[n] = -1 end
        f.entries, f.data_entries, f.max_length = nz(r[1][1]), nz(r[1][2]), nz(r[1][4])
        f.maybe_dateish, f.n_zoned = nz(r[1][5]), nz(r[1][6])
        f.def_cat = nil
        if not isnull(r[1][3]) then f.def_cat = r[1][3] end
        return true, f
    end

    -- Runs one fact query (kind = MAIN or a verification kind) and returns ok, facts | error message.
    local function run_facts(ctx, kind, sampled)
        local o = stmt_options(ctx, sampled)
        local h = stmt_hint(ctx, sampled, o)
        if kind == 'MAIN' and o.witness and h ~= nil and h.w ~= nil then
            local okw, fw = witness_facts(ctx, sampled, o, h)
            if okw then return true, fw end
        end
        local names = AGG_LIST[kind]
        local sql = facts_sql(ctx, kind, sampled, o, h)
        local ok, r = pquery(sql, pick_binds(sql, base_binds(ctx)))
        if not ok and o.any then                       -- an optimized statement failed: the reference statement decides
            sql = facts_sql(ctx, kind, sampled, NOOPT, nil)
            ok, r = pquery(sql, pick_binds(sql, base_binds(ctx)))
        end
        if not ok then return false, (r.error_message or 'error') end
        local f = {}
        for i, n in ipairs(names) do
            if n == 'def_cat' then
                if not isnull(r[1][i]) then f.def_cat = r[1][i] end
            else
                f[n] = nz(r[1][i])
            end
        end
        return true, f
    end

    -- Full column (plus the DEFAULT): ok, number of values, number of values that do NOT parse with the
    -- TIMESTAMP format fmt | error message. Used before a column with DATE values becomes a TIMESTAMP.
    local function count_ts_misfits(ctx, fmt)
        local sql = [[SELECT COUNT(col), SUM(CASE WHEN IS_TIMESTAMP(col, :tsf) THEN 0 ELSE 1 END) FROM (]]
                 .. source_sql(ctx, false) .. [[) WHERE col IS NOT NULL]]
        local binds = base_binds(ctx)
        binds.tsf = fmt
        local ok, r = pquery(sql, pick_binds(sql, binds))
        if not ok then return false, (r.error_message or 'error') end
        return true, nz(r[1][1]), nz(r[1][2])
    end

    ------------------------------------------------------------------------------------------------------
    -- Multi-format date/timestamp probe with EXPLICIT format models (independent of the session NLS). It runs
    -- only for unclassified columns whose values look date-like. A format matches when it parses EVERY
    -- non-null value; a value without a four-digit year counts as a two-digit year. A value has a time
    -- (has_time) when it contains a colon or more than the 3 digit groups of a probed date (r_ptime; IS_DATE also
    -- accepts a trailing hour such as '2000-01-02 10'); when any value has one, only the TIMESTAMP formats are
    -- read, so a DATE format never drops a time of day.
    ------------------------------------------------------------------------------------------------------
    local CAND = { 'YYYY-MM-DD', 'YYYY.MM.DD', 'YYYY/MM/DD', 'DD.MM.YYYY', 'MM.DD.YYYY',
                   'DD/MM/YYYY', 'MM/DD/YYYY', 'DD-MM-YYYY', 'MM-DD-YYYY' }
    local TCAND = {}                                   -- timestamp candidates: date part + separator
    for _, f in ipairs(CAND) do TCAND[#TCAND + 1] = { date = f, sep = ' ' } end
    TCAND[#TCAND + 1] = { date = 'YYYY-MM-DD', sep = 'T' }   -- ISO 8601 (T = ISO date-time separator element)

    local function explicit_ts_format(date, sep, fp)
        local f = date .. sep .. 'HH24:MI:SS'
        if fp > 0 then f = f .. '.FF' .. fp end
        return f
    end

    -- probe_distinct (+ probe_pilot): the same probe facts over the DISTINCT non-null values, every count
    -- weighted with the rows of the value (cnt), so all counts are exact. pick_format reads the DATE counts
    -- (df) only when no value has a time (has_time = 0) and the TIMESTAMP counts (tf) only when one has, so
    -- only that set is parsed (hta = has_time of the whole set; the other set stays 0 and is never read).
    -- Pilot: a format's count is only compared with entries ("parses EVERY value"). A format that fails on
    -- one of the first <= 1000 distinct values (ROW_NUMBER() <= 1000 over the SAME derived set in the SAME
    -- statement, so a pilot value is always one of the counted values) has a count < entries anyway; it gets
    -- 0 (also < entries, since entries >= 1 whenever the counts are read) and is not parsed for the other
    -- values. A format that passes the pilot is counted exactly. need = {kind, fmt}: the one format whose
    -- exact count a note reports; it is always counted for every value.
    local function probe_distinct_sql(ctx, sampled, need, o, binds)
        local pilot = (o.probe_pilot == true)
        local l1 = [[SELECT col, cnt, CASE WHEN INSTR(col, CHR(58)) > 0 OR col REGEXP_LIKE :r_ptime THEN 1 ELSE 0 END ht]] .. (pilot and [[, ROW_NUMBER() OVER () rn]] or '')
                .. [[ FROM (SELECT col, COUNT(*) cnt FROM (SELECT col FROM (]] .. source_sql(ctx, sampled) .. [[) WHERE col IS NOT NULL) GROUP BY col)]]
        local l2 = [[SELECT col, cnt, ht, ]] .. (pilot and 'rn, ' or '') .. [[MAX(ht) OVER () hta FROM (]] .. l1 .. [[)]]
        local sel = { "SUM(cnt) entries", "MAX(ht) has_time", "MAX(" .. SQL_TIME_FRAC .. ") maxfrac",
                      "SUM(CASE WHEN col REGEXP_LIKE :r_y4 THEN 0 ELSE cnt END) n_short",
                      "SUM(CASE WHEN INSTR(col, 'T') > 0 THEN cnt ELSE 0 END) n_tsep",
                      "MAX(CASE WHEN col REGEXP_LIKE :r_pmid THEN 0 ELSE 1 END) nonmid" }
        local pw = {}
        local function add(set, k, ht, test)
            local gate = "hta = " .. ht
            if pilot then
                pw[#pw + 1] = "COALESCE(MIN(CASE WHEN rn <= 1000 AND hta = " .. ht .. " THEN CASE WHEN " .. test .. " THEN 1 ELSE 0 END END) OVER (), 1) p" .. set .. k
                gate = gate .. " AND p" .. set .. k .. " = 1"
            end
            sel[#sel + 1] = "SUM(CASE WHEN " .. gate .. " THEN CASE WHEN " .. test .. " THEN cnt ELSE 0 END ELSE 0 END) " .. set .. k
        end
        for i, f in ipairs(CAND) do
            binds['df' .. i] = f
            add('df', i, 0, "IS_DATE(col, :df" .. i .. ")")
        end
        for j, t in ipairs(TCAND) do
            binds['tf' .. j] = explicit_ts_format(t.date, t.sep, 9)
            add('tf', j, 1, "IS_TIMESTAMP(col, :tf" .. j .. ")")
        end
        if need ~= nil then
            binds.needf = need.fmt
            if need.kind == 'TS' then sel[#sel + 1] = "SUM(CASE WHEN IS_TIMESTAMP(col, :needf) THEN cnt ELSE 0 END) need_hits"
            else sel[#sel + 1] = "SUM(CASE WHEN IS_DATE(col, :needf) THEN cnt ELSE 0 END) need_hits" end
        end
        local from = l2
        if pilot then from = [[SELECT col, cnt, ht, hta, ]] .. table.concat(pw, ', ') .. [[ FROM (]] .. l2 .. [[)]] end
        return [[SELECT ]] .. table.concat(sel, ', ') .. [[ FROM (]] .. from .. [[)]]
    end

    -- Returns ok, probe facts {entries, has_time, frac, n_short, n_tsep, nonmid, df = {}, tf = {}, need} | error message.
    -- nonmid = some value has a digit other than 0 after the 3 digit groups of the date (r_pmid: a time of day other
    -- than midnight in every explicit format, which all read HH24:MI:SS[.FFn]); false = every time is 00:00:00.
    -- need (optional, see probe_distinct_sql): the format whose exact number of matching values a note reports.
    local function run_probe(ctx, sampled, need)
        local o = stmt_options(ctx, sampled)
        local binds = base_binds(ctx)
        local sel = { "COUNT(col) entries",
                      "MAX(CASE WHEN INSTR(col, CHR(58)) > 0 OR col REGEXP_LIKE :r_ptime THEN 1 ELSE 0 END) has_time",
                      "MAX(" .. SQL_TIME_FRAC .. ") maxfrac",
                      "SUM(CASE WHEN col REGEXP_LIKE :r_y4 THEN 0 ELSE 1 END) n_short",
                      "SUM(CASE WHEN INSTR(col, 'T') > 0 THEN 1 ELSE 0 END) n_tsep",
                      "MAX(CASE WHEN col REGEXP_LIKE :r_pmid THEN 0 ELSE 1 END) nonmid" }
        local NFIX = #sel
        for i, f in ipairs(CAND) do
            binds['df' .. i] = f
            sel[#sel + 1] = "SUM(CASE WHEN IS_DATE(col, :df" .. i .. ") THEN 1 ELSE 0 END) df" .. i
        end
        for j, t in ipairs(TCAND) do
            binds['tf' .. j] = explicit_ts_format(t.date, t.sep, 9)
            sel[#sel + 1] = "SUM(CASE WHEN IS_TIMESTAMP(col, :tf" .. j .. ") THEN 1 ELSE 0 END) tf" .. j
        end
        local sql = [[SELECT ]] .. table.concat(sel, ', ') .. [[ FROM (SELECT col, src_row FROM (]]
                 .. source_sql(ctx, sampled) .. [[) WHERE col IS NOT NULL)]]
        local ok, r = false, nil
        local with_need = false
        if o.probe_distinct then
            local dsql = probe_distinct_sql(ctx, sampled, need, o, binds)
            ok, r = pquery(dsql, pick_binds(dsql, binds))
            with_need = ok and need ~= nil
        end
        if not ok then ok, r = pquery(sql, pick_binds(sql, binds)) end      -- the reference statement
        if not ok then return false, (r.error_message or 'error') end
        local row = r[1]
        local p = { entries = nz(row[1]), has_time = (nz(row[2]) == 1), frac = nz(row[3]), n_short = nz(row[4]),
                    n_tsep = nz(row[5]), nonmid = (nz(row[6]) ~= 0), df = {}, tf = {} }
        for i = 1, #CAND  do p.df[i] = nz(row[NFIX + i]) end
        for j = 1, #TCAND do p.tf[j] = nz(row[NFIX + #CAND + j]) end
        if with_need then p.need = nz(row[NFIX + #CAND + #TCAND + 1]) end
        return true, p
    end

    -- Interprets probe facts: {} (nothing matches) | {short = names} | {ambiguous = names} | {kind, date, sep, frac, idx}
    -- The ' ' and the ISO 'T' variant of a timestamp format are never counted as two matches: the 'T' variant
    -- needs a 'T' in every value, the ' ' variant needs a 'T' in none (in case the parser is lenient).
    local function pick_format(p)
        if p.entries == 0 then return {} end
        local matched = {}
        if p.has_time then
            for j, t in ipairs(TCAND) do
                local sep_ok = (t.sep == 'T' and p.n_tsep == p.entries) or (t.sep ~= 'T' and p.n_tsep == 0)
                if sep_ok and p.tf[j] == p.entries then matched[#matched + 1] = { kind = 'TS', date = t.date, sep = t.sep, idx = j } end
            end
        else
            for i, f in ipairs(CAND) do
                if p.df[i] == p.entries then matched[#matched + 1] = { kind = 'DATE', date = f, sep = '', idx = i } end
            end
        end
        if #matched == 0 then return {} end
        local names = {}
        for _, m in ipairs(matched) do
            if m.kind == 'TS' then names[#names + 1] = explicit_ts_format(m.date, m.sep, 0) else names[#names + 1] = m.date end
        end
        if p.n_short > 0 then                                                             -- two-digit years
            -- the values have no four-digit year, so they only fit the two-digit form of a format ('12.06.26' fits
            -- DD.MM.YY and YY.MM.DD, never DD.MM.YYYY or YYYY.MM.DD); the note names these forms
            local yy = {}
            for k, nm in ipairs(names) do yy[k] = (string.gsub(nm, 'YYYY', 'YY')) end
            return { short = table.concat(yy, "', '"), n_forms = #yy, n_short = p.n_short, entries = p.entries }
        end
        if #matched > 1 then return { ambiguous = table.concat(names, "', '") } end      -- any 2+ matches are ambiguous
        local m = matched[1]
        m.frac = p.frac
        m.nonmid = p.nonmid
        return m
    end

    ------------------------------------------------------------------------------------------------------
    -- Target types
    ------------------------------------------------------------------------------------------------------
    -- Exasol type string of a target descriptor, nil = keep. A NUM target never becomes DOUBLE. A TS target with
    -- mid = true (every value with a time part is at midnight) ends as DATE: it is read as TIMESTAMP(fp) with the
    -- exact TIMESTAMP format and then changed to DATE (render_change).
    local function render_type(t)
        if t.fam == 'NUM' then
            if t.idig + t.scale > 36 then return nil end
            if t.scale == 0 then return "DECIMAL(" .. adjust_precision(t.idig) .. ", 0)" end
            return "DECIMAL(" .. adjust_precision(t.idig + t.scale) .. ", " .. t.scale .. ")"
        elseif t.fam == 'DBL'  then return "DOUBLE PRECISION"
        elseif t.fam == 'DATE' then return "DATE"
        elseif t.fam == 'TS' and t.mid then return "DATE"
        elseif t.fam == 'TS'   then return "TIMESTAMP(" .. t.fp .. ")"
        elseif t.fam == 'BOOL' then return "BOOLEAN"
        elseif t.fam == 'DSI'  then return "INTERVAL DAY(" .. t.p .. ") TO SECOND(" .. t.fp .. ")"
        elseif t.fam == 'YMI'  then return "INTERVAL YEAR(" .. t.p .. ") TO MONTH"
        elseif t.fam == 'GEO'  then return "GEOMETRY"
        elseif t.fam == 'VC'   then return "VARCHAR(" .. t.len .. ") " .. (t.ascii and 'ASCII' or 'UTF8')
        end
        return nil
    end

    -- The NLS format a DATE / TIMESTAMP target is parsed with (explicit format or session format).
    local function target_format(t)
        if t.fam == 'DATE' then return t.xdate or nls_date_format end
        if t.xdate ~= nil then return explicit_ts_format(t.xdate, t.xsep, t.fp) end
        return session_ts_format(t.fp)
    end

    -- ALTER SESSION prefix that makes a statement independent of the executing session.
    local function nls_prefix(t)
        if t.fam == 'DATE' or t.fam == 'TS' then
            local f = target_format(t)
            local p = ""
            if nls_date_language ~= nil and uses_names(f) then
                p = "ALTER SESSION SET NLS_DATE_LANGUAGE = " .. sql_str(nls_date_language) .. "; "
            end
            if t.fam == 'DATE' then return p .. "ALTER SESSION SET NLS_DATE_FORMAT = " .. sql_str(f) .. "; " end
            return p .. "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = " .. sql_str(f) .. "; "
        elseif t.fam == 'DBL' or (t.fam == 'NUM' and t.nls_num) then
            return "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = " .. sql_str(nls_numeric) .. "; "
        end
        return ""
    end

    -- Tightest COMMON target of two key-group members, or {fam='KEEP', why=...}. Never DOUBLE.
    local function merge_targets(a, b)
        if a.fam == 'DBL' or b.fam == 'DBL' then
            return { fam = 'KEEP', why = 'a common type would be DOUBLE PRECISION, which is approximate for key values' }
        end
        if a.fam == 'NUM' and b.fam == 'NUM' then
            local t = { fam = 'NUM', idig = math.max(a.idig, b.idig), scale = math.max(a.scale, b.scale),
                        nls_num = (a.nls_num or b.nls_num) }
            if t.idig + t.scale > 36 then
                return { fam = 'KEEP', why = 'a common DECIMAL would need ' .. (t.idig + t.scale) .. ' digits (max 36)' }
            end
            return t
        end
        local da = (a.fam == 'DATE' or a.fam == 'TS')
        local db = (b.fam == 'DATE' or b.fam == 'TS')
        if da and db then
            if a.xdate ~= b.xdate then return { fam = 'KEEP', why = 'the key columns use different date formats' } end
            if a.fam == 'TS' and b.fam == 'TS' and a.xsep ~= b.xsep then
                return { fam = 'KEEP', why = 'the key columns use different date-time separators' }
            end
            if a.fam == 'DATE' and b.fam == 'DATE' then return { fam = 'DATE', xdate = a.xdate } end
            -- DATE via midnight timestamps only when every member is such a target (a DATE member keeps TIMESTAMP)
            return { fam = 'TS', fp = math.max(a.fp or 0, b.fp or 0), xdate = a.xdate,
                     xsep = (a.fam == 'TS' and a.xsep) or b.xsep, mid = (a.mid == true and b.mid == true) or nil }
        end
        if a.fam ~= b.fam then return { fam = 'KEEP', why = 'the key columns have no common type' } end
        if a.fam == 'BOOL' or a.fam == 'GEO' then return { fam = a.fam } end
        if a.fam == 'DSI' then return { fam = 'DSI', p = math.max(a.p, b.p), fp = math.max(a.fp, b.fp) } end
        if a.fam == 'YMI' then return { fam = 'YMI', p = math.max(a.p, b.p) } end
        if a.fam == 'VC'  then return { fam = 'VC', len = math.max(a.len, b.len), ascii = (a.ascii and b.ascii) } end
        return { fam = 'KEEP', why = 'the key columns have no common type' }
    end

    ------------------------------------------------------------------------------------------------------
    -- Column DEFAULT
    ------------------------------------------------------------------------------------------------------
    -- Returns kind, text, value: 'none' | 'literal' (a quoted string or an integer literal; value = the stored
    -- text) | 'expr' (anything else, e.g. CURRENT_USER or 'a' || 'b').
    local function classify_default(d)
        if isnull(d) then return 'none' end
        local t = trim(tostring(d))
        if t == '' or string.upper(t) == 'NULL' then return 'none' end
        if string.match(t, "^'.*'$") ~= nil then
            local raw = string.sub(t, 2, -2)
            if string.find((string.gsub(raw, "''", '')), "'", 1, true) ~= nil then return 'expr', t end
            local v = string.gsub(raw, "''", "'")
            if v == '' then return 'none' end                               -- DEFAULT '' is NULL in Exasol
            return 'literal', t, v
        end
        local body = t
        local c1 = string.sub(body, 1, 1)
        if c1 == '+' or c1 == '-' then body = string.sub(body, 2) end
        if string.match(body, '^%d+$') ~= nil then return 'literal', t, t end
        return 'expr', t
    end

    -- An unquoted decimal or exponent number such as 1.5, -.5 or 1E3 (a numeric literal, but not an integer).
    local function is_number_default(t)
        local b = t
        local c1 = string.sub(b, 1, 1)
        if c1 == '+' or c1 == '-' then b = string.sub(b, 2) end
        local e = string.find(b, '[eE]')
        if e ~= nil then
            local ex = string.sub(b, e + 1)
            local x1 = string.sub(ex, 1, 1)
            if x1 == '+' or x1 == '-' then ex = string.sub(ex, 2) end
            if string.match(ex, '^%d+$') == nil then return false end
            b = string.sub(b, 1, e - 1)
        end
        return string.match(b, '^%d*%.%d*$') ~= nil and string.match(b, '%d') ~= nil
    end

    -- A numeric value text as an NLS-independent SQL numeric literal ('-007,50' -> -7.50, '.5' -> 0.5).
    local function numeric_literal(v)
        local s = trim(v)
        local sign = ''
        local c1 = string.sub(s, 1, 1)
        if c1 == '+' or c1 == '-' then
            if c1 == '-' then sign = '-' end
            s = trim(string.sub(s, 2))
        end
        local mant, ex = s, nil
        local m, e = string.match(s, '^(.-)[eE](.*)$')
        if m ~= nil then mant, ex = m, e end
        local ip, fp = mant, ''
        local pos = string.find(mant, dec_char, 1, true)
        if pos ~= nil then ip = string.sub(mant, 1, pos - 1); fp = string.sub(mant, pos + 1) end
        ip = string.gsub(ip, '^0+', '')
        if ip == '' then ip = '0' end
        local lit = sign .. ip
        if fp ~= '' then lit = lit .. '.' .. fp end
        if ex ~= nil then lit = lit .. 'E' .. string.gsub(ex, '^%+', '') end
        if string.match(lit, '^%-*%d+[%.%d]*[E%-%d]*$') == nil then return nil end
        return lit
    end

    -- "ALTER ... SET DEFAULT <typed literal>;" for a literal DEFAULT of an NLS-dependent target, or nil.
    local function default_statement(a, t)
        if a.defval == nil then return nil end
        local lit = nil
        if t.fam == 'NUM' or t.fam == 'DBL' then
            lit = numeric_literal(a.defval)
        elseif t.fam == 'DATE' then
            -- a DEFAULT with a time part is classified like a value, so it never reaches a DATE target; the guard
            -- only makes sure that TO_DATE can never drop a time of day here (NULL = no literal, see below); with a
            -- session date format that has time elements (date_fmt_time) the DEFAULT must parse at midnight
            local mid = ""
            if t.xdate == nil and date_fmt_time then
                mid = [[ WHEN NOT IS_TIMESTAMP(:v, :f) THEN NULL WHEN TO_TIMESTAMP(:v, :f) <> TRUNC(TO_TIMESTAMP(:v, :f)) THEN NULL]]
            end
            local ok, r = pquery([[SELECT CASE WHEN :v REGEXP_LIKE :t THEN NULL]] .. mid .. [[ ELSE TO_CHAR(TO_DATE(:v, :f), 'YYYY-MM-DD') END FROM SYS.DUAL]],
                                 { v = a.defval, f = target_format(t), t = (t.xdate ~= nil) and RX.r_ptime or RX.r_dtime })
            if ok and #r == 1 and not isnull(r[1][1]) then lit = "DATE " .. sql_str(r[1][1]) end
        elseif t.fam == 'TS' and t.mid then
            -- the column ends as DATE: the DEFAULT was one of the values, so it is a date or a timestamp at midnight;
            -- the guard gives no literal (NULL) for any other time of day, so a DATE literal never drops a time
            local ok, r = pquery([[SELECT CASE WHEN TO_TIMESTAMP(:v, :f) = TRUNC(TO_TIMESTAMP(:v, :f)) THEN TO_CHAR(TO_TIMESTAMP(:v, :f), 'YYYY-MM-DD') END FROM SYS.DUAL]],
                                 { v = a.defval, f = target_format(t) })
            if ok and #r == 1 and not isnull(r[1][1]) then lit = "DATE " .. sql_str(r[1][1]) end
        elseif t.fam == 'TS' then
            local ok, r = pquery([[SELECT TO_CHAR(TO_TIMESTAMP(:v, :f), :o) FROM SYS.DUAL]],
                                 { v = a.defval, f = target_format(t), o = 'YYYY-MM-DD HH24:MI:SS.FF9' })
            if ok and #r == 1 and not isnull(r[1][1]) then lit = "TIMESTAMP " .. sql_str(r[1][1]) end
        else
            return nil                       -- BOOLEAN / INTERVAL / GEOMETRY / VARCHAR: no NLS dependency
        end
        if lit == nil then
            return nil, "Set the DEFAULT " .. a.deftext .. " yourself as a typed literal after the change (it could not be converted here)."
        end
        return "ALTER TABLE " .. quote(a.sch) .. "." .. quote(a.tab) .. " ALTER COLUMN " .. quote(a.col) .. " SET DEFAULT " .. lit .. ";",
               "The column DEFAULT " .. a.deftext .. " is re-set as " .. lit .. ", so new rows do not depend on the NLS settings of the inserting session."
    end

    ------------------------------------------------------------------------------------------------------
    -- Column records
    ------------------------------------------------------------------------------------------------------
    local function add_note(a, s) if s ~= nil and s ~= '' then a.notes[#a.notes + 1] = s end end
    local function drop_note(a, s)
        local r = {}
        for _, x in ipairs(a.notes) do if x ~= s then r[#r + 1] = x end end
        a.notes = r
    end

    local function keep(a, reason, note)
        a.tgt        = { fam = 'KEEP' }
        a.conversion = "Keep " .. a.src_type .. reason
        add_note(a, note)
        return a
    end

    local function could_not(a, msg)
        a.tgt        = { fam = 'KEEP' }
        a.conversion = "Could not analyze " .. a.src_type
        a.notes      = { msg }
        a.notice     = true                  -- shown also with log_for_all_columns = false
        return a
    end

    -- Writes conversion / query_text of a record that converts to target t (the per-column target or the
    -- harmonized target of its FOREIGN KEY key group); tag is appended to the conversion text.
    local function render_change(a, t, tag)
        local dsql, dnote = default_statement(a, t)
        if t.fam == 'TS' and t.mid and a.defval ~= nil and dsql == nil then
            -- guard only (the DEFAULT was one of the midnight values): without a DATE literal for the DEFAULT the
            -- change to DATE would have to convert the DEFAULT text, so the column gets the TIMESTAMP instead
            t = { fam = 'TS', fp = t.fp, xdate = t.xdate, xsep = t.xsep }
            dsql, dnote = default_statement(a, t)
        end
        local ttype = render_type(t)
        local modify = "ALTER TABLE " .. quote(a.sch) .. "." .. quote(a.tab) .. " MODIFY COLUMN " .. quote(a.col) .. " "
        if t.fam == 'TS' and t.mid then
            -- every value is a date or a timestamp at midnight: read every value with the exact TIMESTAMP format
            -- (no reliance on a lenient DATE parse), then TIMESTAMP -> DATE drops only the time 00:00:00. A literal
            -- DEFAULT is re-set as DATE '...' BEFORE the change to DATE: the change converts a DEFAULT text with
            -- NLS_DATE_FORMAT, which need not fit it (measured on Exasol 2025.1: '13.01.2000 00:00:00' under
            -- 'YYYY-MM-DD' gives "can not convert default value to new column type")
            a.query_text = nls_prefix(t) .. modify .. "TIMESTAMP(" .. t.fp .. "); " .. ((dsql ~= nil) and (dsql .. " ") or "")
                        .. modify .. ttype .. ";"
        else
            a.query_text = nls_prefix(t) .. modify .. ttype .. ";"
            if dsql ~= nil then a.query_text = a.query_text .. " " .. dsql end
        end
        add_note(a, dnote)
        local extra = ""
        if t.fam == 'TS' and t.mid then
            extra = " (read as TIMESTAMP(" .. t.fp .. ") with format " .. target_format(t) .. "; every time of day is 00:00:00)"
        elseif (t.fam == 'DATE' or t.fam == 'TS') and t.xdate ~= nil then extra = " (format " .. target_format(t) .. ")" end
        if t.fam == 'VC' then
            if a.nodata then extra = ", no value" else extra = ", max length: " .. a.max_length end   -- key member without data
        end
        a.conversion = a.src_type .. " --> " .. ttype .. extra .. (tag or "")
        a.has_change = true
    end

    ------------------------------------------------------------------------------------------------------
    -- Decision
    ------------------------------------------------------------------------------------------------------
    -- No single type fits all values: the VARCHAR shrink with the width of the longest value of the FULL
    -- column (incl. the DEFAULT), character set kept; a column of width 3 or less, or one where the shrink
    -- gains nothing, is kept. why (optional) names the mix of values.
    local function shrink_or_keep(a, ctx, why)
        local maxlen  = a.max_length
        local new_len = estimate_optimal_varchar_length(maxlen)
        if ctx.col_len > 3 and new_len < ctx.col_len then
            a.tgt = { fam = 'VC', len = new_len, ascii = (ctx.charset == 'ASCII') }
            add_note(a, "No single data type fits all values" .. (why and (" (" .. why .. ")") or "") .. ". Width reduced to the longest value (" .. maxlen .. " characters, measured over the full column incl. the DEFAULT) + 20% headroom, rounded up; character set kept.")
        else
            keep(a, ", max length: " .. maxlen, why and ("No single data type fits all values (" .. why .. ").") or nil)
        end
        return a
    end

    local FAM_LABEL = { NUM = 'DECIMAL', BOOL01 = 'DECIMAL / BOOLEAN (0/1)', DBL = 'DOUBLE PRECISION', DT = 'DATE / TIMESTAMP',
                        BOOL = 'BOOLEAN', DSI = 'INTERVAL DAY TO SECOND', YMI = 'INTERVAL YEAR TO MONTH', GEO = 'GEOMETRY',
                        PERIOD = 'calendar months (YYYY-MM)', ZONED = 'timestamps with a time zone' }
    -- value classes of the fitting values per family (to say whether the column DEFAULT is one of the misfits)
    local FAM_CATS  = { NUM = {INT = true, DEC = true}, BOOL01 = {INT = true, DEC = true},
                        DBL = {INT = true, DEC = true, DBL = true}, DT = {DATE = true, TS = true},
                        DSI = {DSI = true}, YMI = {YMI = true}, GEO = {GEO = true}, PERIOD = {YMI = true, YMP = true} }

    -- Family the facts point to: a convertible family, 'PERIOD', 'ZONED' or 'OTHER'.
    local function propose_family(f)
        local n = f.entries
        if n == f.n_int then
            if n == f.n_bool01 then return 'BOOL01' end
            return 'NUM'
        end
        if n == f.n_int + f.n_dec then return 'NUM' end
        if n == f.n_int + f.n_dec + f.n_dbl then return 'DBL' end
        if n == f.n_date + f.n_ts then return 'DT' end
        if n == f.n_bool then return 'BOOL' end
        if n == f.n_dsi then return 'DSI' end
        if n == f.n_ymi then return 'YMI' end
        if f.n_period > 0 and n == f.n_ymi + f.n_period then return 'PERIOD' end
        if n == f.n_geo then return 'GEO' end
        if f.n_zoned > 0 and n == f.n_date + f.n_ts + f.n_zoned then return 'ZONED' end
        return 'OTHER'
    end

    -- Number of values that fit family fam (compare with v.entries).
    local function family_fits(fam, v)
        if     fam == 'NUM' or fam == 'BOOL01' then return v.n_int + v.n_dec
        elseif fam == 'DBL'    then return v.n_int + v.n_dec + v.n_dbl
        elseif fam == 'DT'     then return v.n_date + v.n_ts
        elseif fam == 'BOOL'   then return v.n_bool
        elseif fam == 'DSI'    then return v.n_dsi
        elseif fam == 'YMI'    then return v.n_ymi
        elseif fam == 'GEO'    then return v.n_geo
        elseif fam == 'PERIOD' then return v.n_ymi + v.n_period
        elseif fam == 'ZONED'  then return v.n_date + v.n_ts + v.n_zoned
        end
        return 0
    end

    -- "N of M values" that do not fit family fam, naming the column DEFAULT when it is one of them.
    local function misfit_text(a, fam, v)
        local n = v.entries
        local what = (n - family_fits(fam, v)) .. " of " .. n .. " values"
        local cats = FAM_CATS[fam]
        if cats ~= nil and v.def_cat ~= nil and not cats[v.def_cat] then what = what .. " (incl. the column DEFAULT " .. a.deftext .. ")" end
        return what
    end

    -- The notes of a DECIMAL target from facts v (leading zeros / '+' sign, trailing zeros, YYYYMMDD).
    local function number_notes(v)
        local r = {}
        if v.n_idlike > 0 then
            r[#r + 1] = "WARNING: " .. v.n_idlike .. " values have leading zeros or a '+' sign (identifier-like: ID / ZIP / phone / article no.). DECIMAL LOSES them ('007' -> 7, '+49' -> 49). Review before applying!"
        end
        if v.n_tzero > 0 then
            r[#r + 1] = "NOTE: " .. v.n_tzero .. " values have trailing zeros after the decimal separator (e.g. '1.10'); as DECIMAL they read 1.1 - the text form (e.g. version numbers) is lost."
        end
        if v.n_int == v.entries and v.n_yyyymmdd == v.entries then
            r[#r + 1] = "NOTE: every value looks like a date in the form YYYYMMDD. If these are dates, convert with TO_DATE(col, 'YYYYMMDD') into a DATE column instead."
        end
        return r
    end
    local MIDNIGHT_NOTE = "Every value with a time part is at midnight (00:00:00, a fraction only of zeros), so DATE keeps every value: query_text first reads the values with the exact TIMESTAMP format (ALTER SESSION SET NLS_TIMESTAMP_FORMAT) as TIMESTAMP, then changes the column to DATE, which drops only the time 00:00:00."
    local NODATA_NOTE = "The column contains no value (full column checked). It may be a candidate for DROP COLUMN; verify before dropping."
    local BOOL01_NOTE = "NOTE: only 0/1 values. Verify these are real booleans, not flags, bits or codes you compute with."
    local BOOL_NOTE   = "NOTE: IS_BOOLEAN accepts TRUE/FALSE, T/F, Y/N, YES/NO, 1/0 and 01/00 (any case); all of them become TRUE / FALSE, the original texts are lost. Verify the column is really a boolean (not a status text you rely on)."

    -- Final decision for a convertible family fam from facts v that are exact for the whole column (a full
    -- scan, or the full-column verification). Sets a.tgt (+ notes) or makes a Keep row.
    local function finalize(a, ctx, fam, v)
        local n = v.entries
        if family_fits(fam, v) < n then        -- guard only: a proposal made from full-column facts always fits
            return keep(a, " (" .. misfit_text(a, fam, v) .. " do not fit " .. FAM_LABEL[fam] .. ")", nil)
        end

        if fam == 'NUM' or fam == 'BOOL01' then
            local idig, scale = v.num_int_digits, v.num_scale
            if fam == 'BOOL01' and v.n_bool01 == n then
                -- BOOLEAN; the numeric target and its notes are kept for a FOREIGN KEY key group, where a 0/1
                -- member merges as a number (see key_target)
                a.tgt = { fam = 'BOOL', from01 = true, num = { fam = 'NUM', idig = idig, scale = scale, nls_num = (v.n_dec > 0) },
                          num_notes = number_notes(v) }
                add_note(a, BOOL01_NOTE)
                return a
            end
            if idig + scale > 36 then
                return keep(a, " (needs " .. (idig + scale) .. " digits > max 36)",
                            "The values need " .. idig .. " integer digits and " .. scale .. " decimals; DECIMAL holds at most 36 digits.")
            end
            a.tgt = { fam = 'NUM', idig = idig, scale = scale, nls_num = (v.n_dec > 0) }
            for _, s in ipairs(number_notes(v)) do add_note(a, s) end
            return a
        end

        if fam == 'DBL' then
            local sig = math.max(v.dbl_sig_digits, v.num_int_digits + v.num_scale)   -- upper bound, conservative
            if sig > 15 or v.n_idlike > 0 or v.n_underflow > 0 then
                local why = {}
                if sig > 15 then why[#why + 1] = "up to " .. sig .. " significant digits (DOUBLE keeps about 15)" end
                if v.n_idlike > 0 then why[#why + 1] = v.n_idlike .. " values with a leading zero or a '+' sign" end
                if v.n_underflow > 0 then why[#why + 1] = v.n_underflow .. " values below the DOUBLE range (they would become 0)" end
                local note = "Not converted to DOUBLE PRECISION: " .. table.concat(why, '; ') .. "."
                if v.n_int + v.n_dec > 0 then
                    -- plain numbers mixed with exponent numbers: no single exact type -> VARCHAR shrink
                    shrink_or_keep(a, ctx, "plain numbers mixed with numbers in exponent notation")
                    add_note(a, note)
                    return a
                end
                return keep(a, " (DOUBLE PRECISION would lose digits)", note)
            end
            a.tgt = { fam = 'DBL' }
            add_note(a, "WARNING: DOUBLE PRECISION is approximate (about 15 significant digits) and drops the text form (e.g. '1E3' -> 1000, '1.50' -> 1.5). Use it only for measured values, never for keys or amounts.")
            return a
        end

        if fam == 'DT' then
            if v.n_short_year > 0 then
                return keep(a, " (two-digit years - not converted)",
                            v.n_short_year .. " values have no four-digit year (e.g. '12.06.26'): the session format reads such a year as 00xx (YYYY) or picks a century itself (YY / RR). Decide the century yourself before converting.")
            end
            if (date_swap ~= nil or ts_swap ~= nil) and v.n_swap == n then
                local sw = date_swap or ts_swap
                if v.n_ts > 0 and ts_swap ~= nil then sw = ts_swap end
                return keep(a, " (ambiguous day/month order - not converted)",
                            "Every value also parses with the swapped format '" .. sw .. "' (every day is <= 12). Pick the order yourself; then run ALTER SESSION SET NLS_DATE_FORMAT / NLS_TIMESTAMP_FORMAT accordingly before the ALTER TABLE ... MODIFY COLUMN.")
            end
            if v.n_ts > 0 then
                local fp = v.ts_frac
                if fp > 9 then
                    return keep(a, " (more than 9 fractional-second digits)", "Values have up to " .. fp .. " fractional-second digits; TIMESTAMP keeps at most 9.")
                end
                local tgt = { fam = 'TS', fp = fp }
                -- every timestamp is at midnight (a time 00:00:00, an all-zero fraction): DATE, read via the exact
                -- TIMESTAMP format (render_change). Not with time elements in NLS_DATE_FORMAT (date_fmt_time): there
                -- the DATE class already holds the midnight values of the date format.
                if not date_fmt_time and v.n_ts_nonmid == 0 then tgt.mid = true end
                if v.n_date > 0 then
                    -- the DATE values were checked with NLS_DATE_FORMAT, but the column is converted with the
                    -- TIMESTAMP format: check every value with exactly that format (full column)
                    local fmt = target_format(tgt)
                    local okt, tot, bad = count_ts_misfits(ctx, fmt)
                    if not okt then return could_not(a, "Check of the TIMESTAMP format failed: " .. tot) end
                    if bad > 0 then
                        shrink_or_keep(a, ctx, "dates mixed with timestamps")
                        add_note(a, "Not converted to TIMESTAMP: the column mixes dates and timestamps, and " .. bad .. " of " .. tot .. " values do not parse with the target format NLS_TIMESTAMP_FORMAT = '" .. fmt .. "' (the dates follow NLS_DATE_FORMAT = '" .. nls_date_format .. "'). Bring them into one format first.")
                        return a
                    end
                end
                a.tgt = tgt
                if tgt.mid then
                    add_note(a, MIDNIGHT_NOTE)
                else
                    add_note(a, "Consider TIMESTAMP WITH LOCAL TIME ZONE if the values are local times that should follow the session time zone.")
                end
            else
                a.tgt = { fam = 'DATE' }
            end
            return a
        end

        if fam == 'BOOL' then
            a.tgt = { fam = 'BOOL' }
            add_note(a, BOOL_NOTE)
            return a
        end

        if fam == 'DSI' then
            local p, fp = math.max(v.dsi_p, 1), v.dsi_fp
            if p > 9 or fp > 9 then
                return keep(a, " (interval precision above 9)", "The values need DAY(" .. p .. ") TO SECOND(" .. fp .. "); the maximum is 9 for both.")
            end
            if fp > 3 then
                return keep(a, " (more than 3 fractional-second digits)",
                            "Values have up to " .. fp .. " fractional-second digits, but INTERVAL DAY TO SECOND keeps only milliseconds (3 digits); the other digits would be cut off without an error. Round the values yourself first if that is acceptable.")
            end
            a.tgt = { fam = 'DSI', p = p, fp = fp }
            return a
        end

        if fam == 'YMI' then
            local p = math.max(v.ymi_p, 1)
            if p > 9 then return keep(a, " (interval precision above 9)", "The values need YEAR(" .. p .. "); the maximum is 9.") end
            a.tgt = { fam = 'YMI', p = p }
            return a
        end

        if fam == 'GEO' then
            a.tgt = { fam = 'GEO' }
            add_note(a, "Consider specifying an SRID (a reference coordinate system; query SYS.EXA_SPATIAL_REF_SYS for possible values).")
            return a
        end
        return keep(a, "", nil)
    end

    -- Column-name hint: whole '_'-separated tokens only.
    local TS_TOKENS   = { TS = true, TIMESTAMP = true, DATETIME = true, TIMSTAMP = true }
    local DATE_TOKENS = { DT = true, DATE = true, DOB = true }
    local function name_hint(colname)
        local kind = nil
        for tok in string.gmatch(string.upper(colname), '[^_]+') do
            if TS_TOKENS[tok] then return 'TS' end
            if DATE_TOKENS[tok] then kind = 'DATE' end
        end
        return kind
    end

    -- Unclassified column: explicit-format probe, then name hint + VARCHAR shrink (or keep). In a sampled
    -- run the probe on the sample only decides whether the full column is probed; the full column decides.
    local function decide_other(a, ctx, f, sampled)
        local fmt_note = nil
        if a.max_length >= 6 and a.max_length <= 40 and f.maybe_dateish == 1 then
            local ok, p = run_probe(ctx, sampled)
            if not ok then return could_not(a, "Date format probe failed: " .. p) end
            local m = pick_format(p)
            if sampled and (p.entries == 0 or m.kind ~= nil or m.short ~= nil or m.ambiguous ~= nil) then
                -- a format that parses every value of the column also parses every sampled value, so a sample
                -- without a match needs no full probe; otherwise the full column decides. The probe draws its
                -- own sample: when that draw holds no value (a sparse column), nothing was checked, so the
                -- full column decides as well.
                local need = nil                       -- the sampled format: its exact count in the full column
                if m.kind == 'TS' then need = { kind = 'TS', fmt = explicit_ts_format(m.date, m.sep, 9) }
                elseif m.kind == 'DATE' then need = { kind = 'DATE', fmt = m.date } end
                local okf, pf = run_probe(ctx, false, need)
                if not okf then return could_not(a, "Verification of the date format failed: " .. pf) end
                local mf = pick_format(pf)
                if m.kind ~= nil and mf.kind == nil and mf.short == nil and mf.ambiguous == nil then
                    local hits = pf.df[m.idx]
                    local fmt  = m.date
                    if m.kind == 'TS' then hits = pf.tf[m.idx]; fmt = explicit_ts_format(m.date, m.sep, 0) end
                    if pf.need ~= nil then hits = pf.need end
                    fmt_note = "The sample matched the format '" .. fmt .. "', but " .. (pf.entries - hits) .. " of " .. pf.entries .. " values in the full column do not match it (checked against the full column)."
                end
                m = mf
            end
            if m.short ~= nil then
                return keep(a, " (two-digit years - not converted)",
                            "Values look like dates with a two-digit year ('" .. m.short .. "'; " .. m.n_short .. " of " .. m.entries
                            .. " values have no four-digit year): a format with YYYY would read '26' as the year 0026, YY / RR pick a century implicitly."
                            .. ((m.n_forms > 1) and " The order of day, month and year is not certain either." or "")
                            .. " Decide the century yourself before converting.")
            end
            if m.ambiguous ~= nil then
                return keep(a, " (ambiguous date format - not converted)",
                            "The values match more than one format ('" .. m.ambiguous .. "'), e.g. the day/month order is ambiguous because every day is <= 12. Pick the format yourself, e.g. ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY'; then ALTER TABLE ... MODIFY COLUMN ... DATE;")
            end
            if m.kind ~= nil then
                if m.kind == 'TS' and m.frac > 9 then
                    return keep(a, " (more than 9 fractional-second digits)", "Values have up to " .. m.frac .. " fractional-second digits; TIMESTAMP keeps at most 9.")
                end
                if m.kind == 'DATE' then
                    a.tgt = { fam = 'DATE', xdate = m.date }
                else
                    a.tgt = { fam = 'TS', fp = m.frac, xdate = m.date, xsep = m.sep }
                    -- every time is 00:00:00: DATE (render_change); not with time elements in NLS_DATE_FORMAT
                    -- (date_fmt_time), whose behaviour stays as it was (see finalize)
                    if m.nonmid == false and not date_fmt_time then a.tgt.mid = true end
                end
                add_note(a, "Values match the explicit format '" .. target_format(a.tgt) .. "' (not the session format); query_text sets it with ALTER SESSION first.")
                if a.tgt.mid then add_note(a, MIDNIGHT_NOTE) end
                return a
            end
        end

        -- no single type: VARCHAR shrink (full-column max length) and/or name hint
        shrink_or_keep(a, ctx, nil)
        add_note(a, fmt_note)
        if f.n_zoned > 0 then
            add_note(a, f.n_zoned .. " values" .. (sampled and " in the sample" or "") .. " look like timestamps with a time zone ('Z' or +hh:mm); they are not converted (a TIMESTAMP would drop the zone).")
        end
        local hint = name_hint(a.col)
        if hint == 'TS' then
            add_note(a, "The name looks like a timestamp, but the values match neither NLS_TIMESTAMP_FORMAT '" .. nls_timestamp_format .. "' nor a known format. If it is a timestamp, normalize it before any width change, e.g. UPDATE " .. quote(a.sch) .. "." .. quote(a.tab) .. " SET " .. quote(a.col) .. " = TO_CHAR(TO_TIMESTAMP(" .. quote(a.col) .. ", '<format of the values>'), 'YYYY-MM-DD HH24:MI:SS.FF9'); then ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'; ALTER TABLE " .. quote(a.sch) .. "." .. quote(a.tab) .. " MODIFY COLUMN " .. quote(a.col) .. " TIMESTAMP(p); (p = fractional-second digits, 0-9)")
        elseif hint == 'DATE' then
            add_note(a, "The name looks like a date, but the values match neither NLS_DATE_FORMAT '" .. nls_date_format .. "' nor a known format. If it is a date, normalize it before any width change, e.g. UPDATE " .. quote(a.sch) .. "." .. quote(a.tab) .. " SET " .. quote(a.col) .. " = TO_CHAR(TO_DATE(" .. quote(a.col) .. ", '<format of the values>'), 'YYYY-MM-DD'); then ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'; ALTER TABLE " .. quote(a.sch) .. "." .. quote(a.tab) .. " MODIFY COLUMN " .. quote(a.col) .. " DATE;")
        end
        if hint ~= nil and a.tgt.fam == 'KEEP' then
            a.conversion = a.conversion .. " (name looks like a " .. ((hint == 'TS') and 'timestamp' or 'date') .. "; values do not parse)"
        end
        return a
    end

    -- Decision for family fam from facts f (exact for the whole column unless sampled = true, which is only
    -- possible for OTHER: the explicit-format probe then verifies against the full column itself).
    local function decide(a, ctx, fam, f, sampled)
        if fam == 'PERIOD' then
            local note = "Values such as '2024-01' are calendar months, not durations; INTERVAL YEAR TO MONTH would read them as 2024 years (and fail for month 12). For a DATE (first day of the month) use TO_DATE(" .. quote(a.col) .. ", 'YYYY-MM') in a new DATE column."
            if f.n_ymi > 0 then                      -- intervals mixed with calendar months: no single type
                shrink_or_keep(a, ctx, f.n_period .. " calendar months 'YYYY-MM' mixed with " .. f.n_ymi .. " intervals")
                add_note(a, note)
                return a
            end
            return keep(a, " (calendar months YYYY-MM - not converted to an interval)", note)
        end
        if fam == 'ZONED' then
            local note = f.n_zoned .. " values carry a time zone ('Z' or +hh:mm). A TIMESTAMP conversion would drop the zone; normalize them to one zone first (e.g. UTC) and remove the designator, then convert."
            if f.n_date + f.n_ts > 0 then            -- timestamps with and without a zone: no single type
                shrink_or_keep(a, ctx, "values with and without a time zone")
                add_note(a, note)
                return a
            end
            return keep(a, " (timestamps with a time zone - not converted)", note)
        end
        if fam == 'OTHER' then return decide_other(a, ctx, f, sampled) end
        return finalize(a, ctx, fam, f)
    end

    ------------------------------------------------------------------------------------------------------
    -- batch_counts / batch_verify: the full-column facts of the columns of ONE sampled table, up to 20
    -- columns per statement. items = { {ctx = , kind = } }: kind = the verification kind of the column's
    -- sampled proposal (batch_verify), or nil when only its data count and MAX(LENGTH) are batched (batch_counts,
    -- or a column without a proposal; its verification then runs per column). Per column the statement computes
    -- exactly the facts of the per-column statements: COUNT(CASE WHEN src_row = 1 THEN col END) and
    -- MAX(LENGTH(col)) of the COUNT / MAX(LENGTH) statement, and the aggregates AGG_LIST[kind] of the
    -- verification statement (aggs() with the column alias instead of col, the same cat_sql). The value
    -- source is the same: every table row (src_row = 1) plus ONE row with src_row = 0 that holds the literal
    -- DEFAULT of every column that has one (CAST(:defval AS VARCHAR(2000000)), as in source_sql) and NULL for
    -- the others. That NULL (typed as the column itself, so the column type is unchanged) is not a value
    -- (COUNT ignores it, every class test on NULL is not TRUE, so it counts 0 everywhere, exactly like the NULLs
    -- of the table rows); the only aggregate that reads the src_row = 0 row itself, def_cat, is not computed
    -- for a column without a DEFAULT (NULL, as without the row). The rows are not grouped (no dedup / witness:
    -- they need one value set per statement); the gates of o apply (they never change a class). A failing
    -- statement leaves ctx.full unset for its columns, so they run the per-column statements (the reference).
    ------------------------------------------------------------------------------------------------------
    local function batch_full_facts(sch, tab, items, o)
        for c0 = 1, #items, 20 do
            local c1 = math.min(c0 + 19, #items)
            local binds = { sch = quote(sch), tab = quote(tab), dec = dec_char, dswap = date_swap, tswap = ts_swap }
            for k, v in pairs(RX) do binds[k] = v end
            local proj, vals, dflt, inner, sel, layout, has_def = {}, {}, {}, {}, {}, {}, false
            for k = c0, c1 do
                local n   = k - c0 + 1
                local it  = items[k]
                local x, c = 'v' .. n, 'k' .. n
                binds['c' .. n] = quote(it.ctx.col)
                proj[n] = '::c' .. n .. ' AS ' .. x
                vals[n] = x
                if it.ctx.defval ~= nil then
                    has_def = true
                    binds['d' .. n] = it.ctx.defval
                    dflt[n] = 'CAST(:d' .. n .. ' AS VARCHAR(2000000))'
                else
                    dflt[n] = 'CAST(NULL AS ' .. it.ctx.src_type .. ')'
                end
                inner[#inner + 1] = x
                local A = aggs(false, o.bool_gate == true, x, c)
                sel[#sel + 1] = A.data_entries
                sel[#sel + 1] = A.max_length
                local names = {}
                if it.kind ~= nil then
                    inner[#inner + 1] = cat_sql(o, x) .. ' AS ' .. c
                    names = AGG_LIST[it.kind]
                    for _, nm in ipairs(names) do
                        if nm == 'def_cat' and it.ctx.defval == nil then sel[#sel + 1] = 'NULL' else sel[#sel + 1] = A[nm] end
                    end
                end
                layout[#layout + 1] = { it = it, names = names }
            end
            local src = 'SELECT ' .. table.concat(proj, ', ') .. ', 1 AS src_row FROM ::sch.::tab'
            if has_def then
                src = 'SELECT ' .. table.concat(vals, ', ') .. ', src_row FROM (' .. src .. ') UNION ALL SELECT '
                   .. table.concat(dflt, ', ') .. ', 0 FROM SYS.DUAL'
            end
            local sql = 'SELECT ' .. table.concat(sel, ', ') .. ' FROM (SELECT ' .. table.concat(inner, ', ') .. ', src_row FROM (' .. src .. '))'
            local ok, r = pquery(sql, pick_binds(sql, binds))
            if ok and #r == 1 then
                local pos = 1
                for _, l in ipairs(layout) do
                    local full = { data_n = nz(r[1][pos]), maxlen = nz(r[1][pos + 1]), kind = l.it.kind }
                    pos = pos + 2
                    if l.it.kind ~= nil then
                        local v = {}
                        for _, nm in ipairs(l.names) do
                            if nm == 'def_cat' then
                                if not isnull(r[1][pos]) then v.def_cat = r[1][pos] end
                            else
                                v[nm] = nz(r[1][pos])
                            end
                            pos = pos + 1
                        end
                        full.v = v
                    end
                    l.it.ctx.full = full
                end
            end
        end
    end

    -- The sampled proposal of a column from its sample facts f: the verification kind, or nil (no value in
    -- the sample, or a family without a verification).
    local function sampled_verify_kind(f)
        if f.data_entries == 0 then return nil end
        return VERIFY_KIND[propose_family(f)]
    end

    -- Full analysis of one column. Any Lua error inside is caught by the caller (pcall), so one column can
    -- only produce a "Could not analyze" row and never aborts the report. The suggested conversion never
    -- depends on the random draw: a sampled proposal is either confirmed by the full column (with the
    -- same classification) or the column is classified again over the full column. ctx.pre_main (the sample
    -- classification) and ctx.full (the batched full-column facts) are set by the batched pass of a sampled
    -- table (batch_counts / batch_verify); without them the column runs its own statements.
    local function analyze_column(ctx, a)
        if ctx.defkind == 'expr' then
            if is_number_default(ctx.deftext) then
                return keep(a, " (unquoted decimal number DEFAULT " .. ctx.deftext .. " - not changed)",
                            "Only a quoted text or an integer DEFAULT is checked against a new type; a DEFAULT written as an unquoted decimal or exponent number (e.g. DEFAULT 1.5) keeps the column. Change the DEFAULT first if you want to convert the column.")
            end
            return keep(a, " (non-literal DEFAULT " .. ctx.deftext .. " - not changed)",
                        "A non-literal DEFAULT cannot be checked against a new type; change the DEFAULT first if you want to convert the column.")
        end
        local sampled = not ctx.full_scan

        -- 1) classification (sample, or the whole table on a full scan)
        local ok, f
        if ctx.pre_main ~= nil then ok, f = ctx.pre_main.ok, ctx.pre_main.f else ok, f = run_facts(ctx, 'MAIN', sampled) end
        if not ok then return could_not(a, f) end

        -- 2) full-column COUNT and MAX(LENGTH); on a full scan they are already exact
        local data_n, maxlen = f.data_entries, f.max_length
        if sampled then
            if ctx.full ~= nil then
                data_n, maxlen = ctx.full.data_n, ctx.full.maxlen
            else
                local sql = [[SELECT COUNT(CASE WHEN src_row = 1 THEN col END), MAX(LENGTH(col)) FROM (]] .. source_sql(ctx, false) .. [[)]]
                local ok2, r2 = pquery(sql, pick_binds(sql, base_binds(ctx)))
                if not ok2 then return could_not(a, r2.error_message or 'error') end
                data_n, maxlen = nz(r2[1][1]), nz(r2[1][2])
            end
        end
        a.max_length = maxlen
        if data_n == 0 then
            -- a key column without data and without a DEFAULT can take the type of the other key-group members
            if ctx.defkind == 'none' then a.nodata = 'every value is NULL' else a.nodata_def = true end
            return keep(a, " (no data - candidate for DROP COLUMN)", NODATA_NOTE)
        end
        if sampled and f.data_entries == 0 then
            -- the sample holds no value of this column: classify the full column instead
            ok, f = run_facts(ctx, 'MAIN', false)
            if not ok then return could_not(a, f) end
            sampled = false
            add_note(a, "The random sample contained no value of this column, so the full column was classified.")
        end

        -- 3) proposal, 4) verification against the full column (sampled runs only)
        local fam = propose_family(f)
        if sampled and VERIFY_KIND[fam] ~= nil then
            local okv, v
            if ctx.full ~= nil and ctx.full.kind == VERIFY_KIND[fam] and ctx.full.v ~= nil then
                okv, v = true, ctx.full.v                       -- computed by the batched pass
            else
                okv, v = run_facts(ctx, VERIFY_KIND[fam], false)
            end
            if not okv then return could_not(a, "Verification against the full column failed: " .. v) end
            if family_fits(fam, v) == v.entries then return decide(a, ctx, fam, v, false) end
            -- 5) misfit: classify the full column (the result a full scan gives)
            add_note(a, "The sample suggested " .. FAM_LABEL[fam] .. ", but " .. misfit_text(a, fam, v) .. " in the full column do not fit; the column was therefore classified over the full column.")
            ok, f = run_facts(ctx, 'MAIN', false)
            if not ok then return could_not(a, "Classification of the full column failed: " .. f) end
            sampled = false
            fam = propose_family(f)
        end
        return decide(a, ctx, fam, f, sampled)
    end

    ------------------------------------------------------------------------------------------------------
    -- Read the VARCHAR columns (base tables only, no views/synonyms/virtual schemas; CHAR is not analyzed)
    ------------------------------------------------------------------------------------------------------
    local suc, res = pquery([[
        SELECT column_schema, column_table, column_name, column_maxsize, column_type, column_default
        FROM   SYS.EXA_ALL_COLUMNS
        WHERE  column_schema      LIKE :schp ESCAPE CHR(92)
           AND column_table       LIKE :tabp ESCAPE CHR(92)
           AND column_type_id     = 12          -- VARCHAR
           AND column_object_type = 'TABLE'
           AND column_is_virtual  = FALSE
        ORDER  BY column_schema, column_table, column_name
    ]], { schp = schp, tabp = tabp })
    if not suc then
        fail('Could not read SYS.EXA_ALL_COLUMNS: ' .. (res.error_message or 'unknown error'))
    end

    ------------------------------------------------------------------------------------------------------
    -- Per-table options and hints (performance only; they never change a fact, see OPT)
    ------------------------------------------------------------------------------------------------------
    -- The options for a statement that reads eff rows (the sample size, or the table row count for a
    -- statement over the full column): none below opt_min_sample, the pilot only from pilot_min_sample on.
    -- batch = batch_counts or batch_verify (read only from the options of the full table).
    local function table_options(eff)
        if eff < OPT.opt_min_sample then return NOOPT end
        local o = { geo_gate = OPT.geo_gate, probe_distinct = OPT.probe_distinct, dedup = OPT.dedup, witness = OPT.witness,
                    bool_gate = OPT.bool_gate, len_gate = OPT.len_gate,
                    probe_pilot = (OPT.probe_pilot and eff >= OPT.pilot_min_sample),
                    batch_counts = (OPT.batch_counts == true), batch_verify = (OPT.batch_verify == true) }
        o.batch = (o.batch_counts or o.batch_verify)
        o.any = (o.geo_gate or o.probe_distinct or o.dedup or o.witness or o.bool_gate or o.len_gate or o.probe_pilot) == true
        if not o.any and not o.batch then return NOOPT end
        return o
    end

    -- Hints per column of one table (20 columns per statement): dedup = few distinct values in the first eff
    -- rows (APPROXIMATE_COUNT_DISTINCT <= 30% of eff); w = a witness value from the first 1000 rows (see
    -- witness_facts). A failing hint statement only means no hint.
    local function compute_hints(sch, tab, cols, eff, full_scan, o)
        local hints = {}
        for _, c in ipairs(cols) do hints[c] = {} end
        for c0 = 1, #cols, 20 do
            local c1 = math.min(c0 + 19, #cols)
            local binds = { sch = quote(sch), tab = quote(tab), thr = math.floor(0.3 * eff) }
            for k, v in pairs(RX) do binds[k] = v end
            local proj, aggd, aggw = {}, {}, {}
            for k = c0, c1 do
                local n = k - c0 + 1
                local v = 'v' .. n
                binds['c' .. n] = quote(cols[k])
                proj[n] = '::c' .. n .. ' AS ' .. v
                aggd[n] = 'CASE WHEN APPROXIMATE_COUNT_DISTINCT(' .. v .. ') <= :thr THEN 1 ELSE 0 END d' .. n
                aggw[n] = 'MIN(CASE WHEN LENGTH(' .. v .. ') <= 1000 THEN CASE WHEN ' .. cat_sql(o, v) .. " = 'OTH' THEN CASE WHEN IS_BOOLEAN(" .. v
                          .. ') OR ' .. v .. ' REGEXP_LIKE :r_zone THEN NULL ELSE ' .. v .. ' END END END) w' .. n
            end
            local src = 'SELECT ' .. table.concat(proj, ', ') .. ' FROM ::sch.::tab'
            if o.dedup then
                local smp = src
                if not full_scan then smp = smp .. ' LIMIT ' .. string.format('%d', eff) end
                local sql = 'SELECT ' .. table.concat(aggd, ', ') .. ' FROM (' .. smp .. ')'
                local ok, r = pquery(sql, pick_binds(sql, binds))
                if ok then
                    for k = c0, c1 do if nz(r[1][k - c0 + 1]) == 1 then hints[cols[k]].dedup = true end end
                end
            end
            if o.witness then
                local sql = 'SELECT ' .. table.concat(aggw, ', ') .. ' FROM (' .. src .. ' LIMIT 1000)'
                local ok, r = pquery(sql, pick_binds(sql, binds))
                if ok then
                    for k = c0, c1 do
                        local w = r[1][k - c0 + 1]
                        if not isnull(w) and type(w) == 'string' then hints[cols[k]].w = w end
                    end
                end
            end
        end
        return hints
    end

    local analyzed = {}

    -- auto_full_scan: a table with at most full_scan_max_rows rows is read completely when the requested sample
    -- covers at least AUTO_MIN_PCT percent of it (see OPT): a percentage sample of at least AUTO_MIN_PCT percent,
    -- or a number of rows n with n * 100 >= AUTO_MIN_PCT * COUNT(*), i.e. COUNT(*) <= n * 100 / AUTO_MIN_PCT
    local auto_limit = nil
    if OPT.auto_full_scan == true and sampled_run then
        if sample_rows > 0 then
            auto_limit = math.min(OPT.full_scan_max_rows, math.floor(sample_rows * 100 / AUTO_MIN_PCT))
        elseif sample_pct >= AUTO_MIN_PCT then
            auto_limit = OPT.full_scan_max_rows
        end
        if auto_limit ~= nil and auto_limit <= 0 then auto_limit = nil end
    end

    local i = 1
    while i <= #res do
        -- the columns i .. j - 1 belong to one table (schema, table) - never the table name alone
        local sch, tab = res[i][1], res[i][2]
        local j = i
        while j <= #res and res[j][1] == sch and res[j][2] == tab do j = j + 1 end
        local tcols = {}
        for k = i, j - 1 do tcols[#tcols + 1] = res[k][3] end

        -- one COUNT(*) per table
        local tab_rows, tab_sample, tab_error = 0, 0, nil
        local csuc, cres = pquery([[SELECT COUNT(*) FROM ::sch.::tab]], { sch = quote(sch), tab = quote(tab) })
        if not csuc then
            tab_error = cres.error_message or 'error'
        else
            tab_rows = nz(cres[1][1])
            if sample_rows > 0 then
                tab_sample = sample_rows
            else
                tab_sample = math.floor(tab_rows * sample_pct / 100)
                if tab_sample < SAMPLE_MIN then tab_sample = math.min(tab_rows, SAMPLE_MIN) end
            end
        end
        local full_scan = (tab_sample >= tab_rows)       -- also for '100%' and small tables
        local auto_full = false
        if not full_scan and auto_limit ~= nil and tab_rows <= auto_limit then full_scan, auto_full = true, true end

        -- options: statements over the sample by the sample size, statements over the full column by the row
        -- count (on a full scan both are the row count); hints of the sample now, hints over the full table on
        -- first use (on a full scan they are the same)
        local eff = full_scan and tab_rows or tab_sample
        local tab_o, tab_of = table_options(eff), table_options(tab_rows)
        local tab_hints = {}
        if tab_error == nil and tab_rows > 0 and (tab_o.dedup or tab_o.witness) then
            local okh, h = pcall(compute_hints, sch, tab, tcols, eff, full_scan, tab_o)
            if okh then tab_hints = h end
        end
        local full_hints = nil
        if full_scan then full_hints = tab_hints end
        local function full_hint(col)
            if full_hints == nil then
                full_hints = {}
                if tab_of.dedup or tab_of.witness then
                    local okh, h = pcall(compute_hints, sch, tab, tcols, tab_rows, true, tab_of)
                    if okh then full_hints = h end
                end
            end
            return full_hints[col]
        end

        local recs = {}
        for k = i, j - 1 do
            local col = res[k][3]
            local col_len = nz(res[k][4])
            local charset = 'UTF8'
            if string.find(string.upper(res[k][5]), 'ASCII', 1, true) ~= nil then charset = 'ASCII' end
            local src_type = "VARCHAR(" .. col_len .. ") " .. charset

            local a = { sch = sch, tab = tab, col = col, src_type = src_type, conversion = '', query_text = '',
                        notes = {}, tgt = { fam = 'KEEP' }, has_change = false, notice = false, max_length = 0 }
            local defkind, deftext, defval = classify_default(res[k][6])
            a.deftext = deftext
            if defkind == 'literal' then a.defval = defval end
            local ctx = { sch = sch, tab = tab, col = col, col_len = col_len, charset = charset, src_type = src_type,
                          defkind = defkind, deftext = deftext, defval = a.defval, full_scan = full_scan,
                          o = tab_o, of = tab_of, hint = tab_hints[col], full_hint = full_hint }
            if not full_scan then
                ctx.p_lit   = string.format('%.15f', tab_sample / tab_rows)
                ctx.cap_lit = string.format('%d', math.floor(tab_sample))
            end
            recs[#recs + 1] = { a = a, ctx = ctx }
        end

        if tab_error == nil and tab_rows > 0 and not full_scan and tab_of.batch then
            -- batch_counts / batch_verify: first the sample classification of every column, then the full-column
            -- facts of all columns in batched statements; a Lua error here only means the per-column statements run
            local items = {}
            for _, rc in ipairs(recs) do
                if rc.ctx.defkind ~= 'expr' then
                    local okp, okm, fm = pcall(run_facts, rc.ctx, 'MAIN', true)
                    if okp then
                        rc.ctx.pre_main = { ok = okm, f = fm }
                        if okm then
                            local kind = nil
                            if tab_of.batch_verify then kind = sampled_verify_kind(fm) end
                            items[#items + 1] = { ctx = rc.ctx, kind = kind }
                        end
                    end
                end
            end
            if #items > 0 then pcall(batch_full_facts, sch, tab, items, tab_of) end
        end

        for _, rc in ipairs(recs) do
            local a, ctx = rc.a, rc.ctx
            if tab_error ~= nil then
                could_not(a, "The table could not be read: " .. tab_error)
            elseif tab_rows == 0 then
                a.conversion = "Table is empty (no data)"
                if ctx.defkind == 'none' then a.nodata = 'the table is empty' else a.nodata_def = true end   -- see fk_handling
            else
                local okc, err = pcall(analyze_column, ctx, a)
                if not okc then
                    a.notes = {}
                    could_not(a, "Internal error while analyzing this column: " .. tostring(err))
                elseif sampled_run then
                    -- per table: was it sampled or read completely (the header row describes the whole call)
                    if auto_full then
                        add_note(a, "Values read: the whole table (" .. string.format('%d', math.floor(tab_rows)) .. " rows; tables up to " .. string.format('%d', auto_limit) .. " rows are read completely when the requested sample covers at least " .. AUTO_MIN_PCT .. "% of them - on the reference cluster a full scan was not slower than a 5% sample at 250,000 and 500,000 rows; the suggestion is the same).")
                    elseif full_scan then
                        add_note(a, "Values read: the whole table (" .. string.format('%d', math.floor(tab_rows)) .. " rows; the requested sample covers it).")
                    else
                        add_note(a, "Values read: a random sample of about " .. ctx.cap_lit .. " of " .. string.format('%d', math.floor(tab_rows)) .. " rows; the suggestion was verified against the full column.")
                    end
                end
            end
            analyzed[#analyzed + 1] = a
        end
        i = j
    end

    ------------------------------------------------------------------------------------------------------
    -- FOREIGN KEY handling. In Exasol a FOREIGN KEY column and the referenced PRIMARY KEY column must have
    -- the same type: (1) every referential key group that lies fully inside the filter is harmonized to one
    -- common target type that fits all its columns; (2) only these key columns are changed inside a
    -- DROP / RE-ADD FOREIGN KEYS block; (3) a key column whose group reaches a table outside the filter is
    -- kept with a note. Columns not involved in any FOREIGN KEY are unaffected.
    ------------------------------------------------------------------------------------------------------
    local note_rows, drop_rows, readd_rows = {}, {}, {}

    -- When the key columns are unknown (catalog not readable, or the FOREIGN KEY handling itself failed), no
    -- statement is given for any column: a key column changed without the DROP / RE-ADD bracket would fail.
    -- The rows still show the suggested type.
    local function withhold_all(reason)
        drop_rows, readd_rows = {}, {}
        for _, a in ipairs(analyzed) do
            if a.tgt.fam ~= 'KEEP' or a.has_change or a.in_fk_block then
                local okt, tt = pcall(render_type, a.tgt)
                local sugg = ""
                if okt and tt ~= nil then sugg = " (suggested type " .. tt .. ")" end
                a.in_fk_block, a.has_change, a.query_text = false, false, ''
                a.tgt        = { fam = 'KEEP' }
                a.conversion = "Not changed: " .. a.src_type .. sugg .. " - " .. reason
                add_note(a, "No statement is given because the FOREIGN KEY key columns are unknown (see the ERROR row); re-run when the FOREIGN KEY catalog is readable.")
                a.notice     = true
            end
        end
    end

    ------------------------------------------------------------------------------------------------------
    -- Text forms that a conversion drops without an error: blanks before or after the value (' 5', a padded
    -- date) and, for DECIMAL, the forms '5.', '.5' and '-0' (they read 5, 0.5 and 0). One statement per table
    -- counts them over the full table for every column whose proposal changes the values (DECIMAL, DOUBLE,
    -- DATE, TIMESTAMP, intervals); the counts only add notes. A failing statement adds no note.
    ------------------------------------------------------------------------------------------------------
    local function text_form_notes()
        local by_tab, order = {}, {}
        for _, a in ipairs(analyzed) do
            local f = a.tgt.fam
            if f == 'NUM' or f == 'DBL' or f == 'DATE' or f == 'TS' or f == 'DSI' or f == 'YMI' then
                local k = a.sch .. string.char(1) .. a.tab
                if by_tab[k] == nil then by_tab[k] = { sch = a.sch, tab = a.tab, cols = {} }; order[#order + 1] = k end
                by_tab[k].cols[#by_tab[k].cols + 1] = a
            end
        end
        -- '5.' / '.5' (no digit on one side of the decimal separator) and a negative zero ('-0', '-0.00')
        local form_re = '([+-]{0,1}([0-9]+' .. dec_re .. '|' .. dec_re .. '[0-9]+)|-(0+(' .. dec_re .. '0*){0,1}|' .. dec_re .. '0+))'
        for _, k in ipairs(order) do
            local d = by_tab[k]
            local sel = {}
            for _, a in ipairs(d.cols) do
                local q = quote(a.col)
                sel[#sel + 1] = "SUM(CASE WHEN LENGTH(" .. q .. ") <> LENGTH(TRIM(" .. q .. ")) THEN 1 ELSE 0 END)"
                if a.tgt.fam == 'NUM' then
                    sel[#sel + 1] = "SUM(CASE WHEN TRIM(" .. q .. ") REGEXP_LIKE :form_re THEN 1 ELSE 0 END)"
                else
                    sel[#sel + 1] = "0"
                end
            end
            local ok, r = pquery("SELECT " .. table.concat(sel, ", ") .. " FROM " .. quote(d.sch) .. "." .. quote(d.tab), { form_re = form_re })
            if ok then
                for i, a in ipairs(d.cols) do
                    local n_pad, n_form = nz(r[1][2 * i - 1]), nz(r[1][2 * i])
                    if n_pad > 0 then
                        local eg = ''
                        if a.tgt.fam == 'NUM' or a.tgt.fam == 'DBL' then eg = " (e.g. ' 5')" end
                        add_note(a, "NOTE: " .. string.format('%d', n_pad) .. " values have blanks before or after the value" .. eg .. "; the converted values have none - the blank padding is lost.")
                    end
                    if n_form > 0 then
                        add_note(a, "NOTE: " .. string.format('%d', n_form) .. " values are written as '5" .. dec_char .. "', '" .. dec_char .. "5' or '-0' (no digit before or after the decimal separator, or a negative zero); as DECIMAL they read 5, 0" .. dec_char .. "5 and 0 - the text form is lost.")
                    end
                end
            end
        end
    end

    ------------------------------------------------------------------------------------------------------
    -- PRIMARY KEY values that collide after the conversion. Different texts can become the same value:
    -- IS_BOOLEAN merges 'Y', '1' and 'yes' into TRUE; as DECIMAL '1', ' 1', '01' and '1.0' are all 1; as DATE
    -- '2020-01-01', ' 2020-01-01' and '2020-01-01 00:00:00' are one day; as TIMESTAMP '10:00:00' and
    -- '10:00:00.0' are one time. Then the MODIFY of a PRIMARY KEY column fails with a primary key constraint
    -- violation (or, with a disabled PRIMARY KEY, leaves duplicate keys). For every PRIMARY KEY with a column
    -- whose proposal changes the values, the distinct key values are counted over the full table with the
    -- original texts and with the converted values (the same format / NLS as the generated statement); when
    -- the converted values collide, these columns are kept. A column with only 0/1 values cannot collide (two
    -- texts, two values) and is not checked; a VARCHAR shrink keeps the texts. Runs before the FOREIGN KEY
    -- handling, so a key group with such a member is kept as a whole. When the constraint catalog cannot be
    -- read, the FOREIGN KEY handling reports it and withholds every statement.
    ------------------------------------------------------------------------------------------------------
    -- The converted value of column a as the generated statement computes it, the type name and the bind
    -- values (the DATE / TIMESTAMP format is bound as :pkf<k>); nil = the proposal keeps the texts.
    local function pk_cast(a, k)
        local t, q = a.tgt, quote(a.col)
        k = k or 0
        if t.fam == 'BOOL' and not t.from01 then return "CAST(" .. q .. " AS BOOLEAN)", "BOOLEAN" end
        if t.fam == 'NUM' or t.fam == 'DBL' or t.fam == 'DSI' or t.fam == 'YMI' then
            local rt = render_type(t)
            if rt == nil then return nil end
            return "CAST(" .. q .. " AS " .. rt .. ")", rt
        end
        if t.fam == 'DATE' then return "TO_DATE(" .. q .. ", :pkf" .. k .. ")", "DATE", target_format(t) end
        if t.fam == 'TS' then return "TO_TIMESTAMP(" .. q .. ", :pkf" .. k .. ")", render_type(t), target_format(t) end
        return nil
    end

    local function pk_key_check()
        local acol = {}
        local any = false
        for _, a in ipairs(analyzed) do
            if pk_cast(a) ~= nil then
                acol[a.sch .. string.char(1) .. a.tab .. string.char(1) .. a.col] = a
                any = true
            end
        end
        if not any then return end
        local pk_sql = [[
            SELECT constraint_schema, constraint_table, constraint_name, column_name
            FROM   SYS.EXA_DBA_CONSTRAINT_COLUMNS
            WHERE  constraint_type = 'PRIMARY KEY'
              AND  constraint_schema LIKE :schp ESCAPE CHR(92)
              AND  constraint_table  LIKE :tabp ESCAPE CHR(92)
            ORDER  BY constraint_schema, constraint_table, constraint_name, ordinal_position
        ]]
        local ok, pk = pquery(pk_sql, { schp = schp, tabp = tabp })
        if not ok then ok, pk = pquery((string.gsub(pk_sql, 'EXA_DBA_', 'EXA_ALL_')), { schp = schp, tabp = tabp }) end
        if not ok then return end
        local pks, order = {}, {}
        for i = 1, #pk do
            local key = pk[i][1] .. string.char(1) .. pk[i][2] .. string.char(1) .. pk[i][3]
            if pks[key] == nil then
                pks[key] = { sch = pk[i][1], tab = pk[i][2], cols = {} }
                order[#order + 1] = key
            end
            pks[key].cols[#pks[key].cols + 1] = pk[i][4]
        end
        for _, key in ipairs(order) do
            local d = pks[key]
            local plain, cast, members, types, binds = {}, {}, {}, {}, {}
            local all_bool = true
            for _, c in ipairs(d.cols) do
                local a = acol[d.sch .. string.char(1) .. d.tab .. string.char(1) .. c]
                plain[#plain + 1] = quote(c)
                local ex, ty, fb = nil, nil, nil
                if a ~= nil then ex, ty, fb = pk_cast(a, #cast + 1) end
                if fb ~= nil then binds['pkf' .. (#cast + 1)] = fb end
                if ex ~= nil then
                    cast[#cast + 1] = ex
                    members[#members + 1] = a
                    types[#types + 1] = ty
                    if ty ~= 'BOOLEAN' then all_bool = false end
                else
                    cast[#cast + 1] = quote(c)
                end
            end
            if #members > 0 then
                -- a Lua error here (never expected) keeps the members of this key, like a failing count
                local okl, okc, r = pcall(function()
                    local tq = quote(d.sch) .. "." .. quote(d.tab)
                    if #plain == 1 then
                        return pquery("SELECT COUNT(DISTINCT " .. plain[1] .. "), COUNT(DISTINCT " .. cast[1] .. ") FROM " .. tq, binds)
                    end
                    return pquery("SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT " .. table.concat(plain, ", ") .. " FROM " .. tq .. ")), "
                               .. "(SELECT COUNT(*) FROM (SELECT DISTINCT " .. table.concat(cast, ", ") .. " FROM " .. tq .. ")) FROM SYS.DUAL", binds)
                end)
                if not okl then okc, r = false, { error_message = tostring(okc) } end
                local n_txt, n_new = nil, nil
                if okc then n_txt, n_new = nz(r[1][1]), nz(r[1][2]) end
                if not okc or n_new < n_txt then
                    local what, label, fix
                    if all_bool then
                        label = "BOOLEAN"
                        fix = "Every value is a boolean text; if the column really is a boolean, remove the PRIMARY KEY or clean up the texts first."
                        if okc then
                            what = "As BOOLEAN the " .. n_txt .. " different PRIMARY KEY values (columns " .. table.concat(plain, ", ") .. ") would give only "
                                   .. n_new .. " different keys, because different texts become the same value (e.g. 'Y' and '1' both become TRUE)"
                        end
                    else
                        -- one type per column (a composite key names each member), one example per type family
                        local per, egs, seen = {}, {}, {}
                        for i, a in ipairs(members) do
                            if #members > 1 then per[#per + 1] = types[i] .. " (column " .. quote(a.col) .. ")" else per[#per + 1] = types[i] end
                            local f = a.tgt.fam
                            if f == 'TS' and a.tgt.mid then f = 'DATEMID' end   -- ends as DATE (midnight timestamps)
                            if not seen[f] then
                                seen[f] = true
                                if f == 'NUM' then egs[#egs + 1] = "'1', ' 1', '01' and '1" .. dec_char .. "0' are the same DECIMAL"
                                elseif f == 'DBL' then egs[#egs + 1] = "'1000', '1e3' and ' 1000' are the same DOUBLE"
                                elseif f == 'DATE' then egs[#egs + 1] = "a date with blanks around it" .. (date_fmt_time and " or with and without a midnight time of the date format" or "") .. " is the same DATE as the plain date"
                                elseif f == 'DATEMID' then egs[#egs + 1] = "a date, the same date at 00:00:00 and a padded value are the same DATE"
                                elseif f == 'TS' then egs[#egs + 1] = "'10:00:00' and '10:00:00" .. dec_char .. "0' (a zero fraction), a padded value, or a date and the same date at 00:00:00 are the same TIMESTAMP"
                                elseif f == 'DSI' then egs[#egs + 1] = "'1 12:00:00' and '+1 12:00:00' are the same INTERVAL DAY TO SECOND"
                                elseif f == 'YMI' then egs[#egs + 1] = "'1-6' and '+1-6' are the same INTERVAL YEAR TO MONTH"
                                elseif f == 'BOOL' then egs[#egs + 1] = "'Y' and '1' both become TRUE"
                                end
                            end
                        end
                        if #per > 1 then label = table.concat(per, ", ", 1, #per - 1) .. " and " .. per[#per] else label = per[1] end
                        fix = "Bring the key texts into one form first (e.g. remove blanks, leading zeros or a zero time part) if you want to convert the column."
                        if okc then
                            what = "As " .. label .. " the " .. n_txt .. " different PRIMARY KEY values (columns " .. table.concat(plain, ", ") .. ") would give only "
                                   .. n_new .. " different keys, because different texts become the same value"
                            if #egs > 0 then what = what .. " (e.g. " .. table.concat(egs, "; ") .. ")" end
                        end
                    end
                    if not okc then
                        what = "The PRIMARY KEY values (columns " .. table.concat(plain, ", ") .. ") could not be checked for duplicates as " .. label .. " ("
                               .. (r.error_message or 'error') .. ")"
                    end
                    for i, a in ipairs(members) do
                        drop_note(a, BOOL_NOTE)
                        keep(a, " (PRIMARY KEY: values would collide as " .. types[i] .. " - not changed)",
                             what .. ". ALTER TABLE ... MODIFY COLUMN " .. quote(a.col) .. " " .. types[i] .. " would fail with a primary key constraint violation, or leave duplicate keys when the PRIMARY KEY is disabled. " .. fix)
                    end
                end
            end
        end
    end

    local function fk_handling()
        local fk_sql = [[
            SELECT cc.constraint_schema, cc.constraint_table, cc.constraint_name, cc.ordinal_position,
                   cc.column_name, cc.referenced_schema, cc.referenced_table, cc.referenced_column,
                   c.constraint_enabled
            FROM   SYS.EXA_DBA_CONSTRAINT_COLUMNS cc
            JOIN   SYS.EXA_DBA_CONSTRAINTS c
                   ON  c.constraint_schema = cc.constraint_schema
                   AND c.constraint_table  = cc.constraint_table
                   AND c.constraint_name   = cc.constraint_name
            WHERE  cc.constraint_type = 'FOREIGN KEY'
              AND  (cc.constraint_schema LIKE :schp ESCAPE CHR(92) OR cc.referenced_schema LIKE :schp ESCAPE CHR(92))
            ORDER  BY cc.constraint_schema, cc.constraint_table, cc.constraint_name, cc.ordinal_position
        ]]
        local fk_ok, fk = pquery(fk_sql, { schp = schp })
        local fk_view = 'SYS.EXA_DBA_CONSTRAINTS'
        if not fk_ok then                                  -- no SELECT ANY DICTIONARY: fall back
            fk_ok, fk = pquery((string.gsub(fk_sql, 'EXA_DBA_', 'EXA_ALL_')), { schp = schp })
            fk_view = 'SYS.EXA_ALL_CONSTRAINTS'
            if fk_ok then
                note_rows[#note_rows + 1] = { '', '', '', 'NOTE: FOREIGN KEYs were read from SYS.EXA_ALL_CONSTRAINTS (SYS.EXA_DBA_CONSTRAINTS is not readable for this user)', '',
                    'FOREIGN KEYs on tables you cannot see are unknown to this report. A suggested change of a PRIMARY KEY column that such a FOREIGN KEY references will fail; ask a user with SELECT ANY DICTIONARY to run the report for complete key groups.' }
            end
        end
        if not fk_ok then                                  -- never skip the FK handling silently
            note_rows[#note_rows + 1] = { '', '', '', 'ERROR: the FOREIGN KEY catalog could not be read', '',
                'Reading ' .. fk_view .. ' failed: ' .. (fk.error_message or 'error') .. '. Key columns could not be identified, so no statement is given for any column (a change of a PRIMARY KEY or FOREIGN KEY column without the FOREIGN KEY block would fail). Re-run when the catalog is readable.' }
            withhold_all("the FOREIGN KEY catalog could not be read")
        elseif #fk > 0 then
            local function nodek(s, t, c) return s .. string.char(1) .. t .. string.char(1) .. c end
            local acol = {}
            for _, a in ipairs(analyzed) do acol[nodek(a.sch, a.tab, a.col)] = a end

            local parent, nodedisp = {}, {}            -- union-find over column nodes
            local function find(x)
                if parent[x] == nil then parent[x] = x end
                while parent[x] ~= x do parent[x] = parent[parent[x]]; x = parent[x] end
                return x
            end
            local function union(x, y)
                local rx, ry = find(x), find(y)
                if rx ~= ry then parent[rx] = ry end
            end
            local fkdef, fkorder = {}, {}
            for i = 1, #fk do
                local cs, ct, cn = fk[i][1], fk[i][2], fk[i][3]
                local cc         = fk[i][5]
                local rs, rt, rc = fk[i][6], fk[i][7], fk[i][8]
                local cnode, rnode = nodek(cs, ct, cc), nodek(rs, rt, rc)
                nodedisp[cnode] = quote(cs) .. '.' .. quote(ct)      -- quote() escapes embedded quotes
                nodedisp[rnode] = quote(rs) .. '.' .. quote(rt)
                union(cnode, rnode)
                local key = nodek(cs, ct, cn)
                if fkdef[key] == nil then
                    fkdef[key] = { csch = cs, ctab = ct, cname = cn, cols = {}, rsch = rs, rtab = rt, rcols = {}, enabled = fk[i][9] }
                    fkorder[#fkorder + 1] = key
                end
                fkdef[key].cols[#fkdef[key].cols + 1]   = cc
                fkdef[key].rcols[#fkdef[key].rcols + 1] = rc
            end

            local groups = {}
            for nkey in pairs(parent) do
                local root = find(nkey)
                if groups[root] == nil then groups[root] = {} end
                groups[root][#groups[root] + 1] = nkey
            end

            -- A key column that is kept for its OWN reason (error, no data, misfit, ...) keeps its own row and
            -- gets the group note appended; any other member gets the group's Keep row. Both are always shown
            -- (only called for groups that reach outside the filter or really block a conversion).
            local function keep_key(a, reason, note)
                a.notice  = true
                a.fk_note = true                     -- listed in the FOREIGN KEY NOTES section
                if a.tgt.fam == 'KEEP' then
                    add_note(a, note)
                    return
                end
                a.tgt, a.query_text, a.has_change = { fam = 'KEEP' }, '', false
                a.conversion = "Keep " .. a.src_type .. reason
                a.notes = { note }
            end

            local converting = {}
            for root, nodes in pairs(groups) do
                local members, missing = {}, {}
                for _, nkey in ipairs(nodes) do
                    if acol[nkey] ~= nil then members[#members + 1] = acol[nkey] else missing[#missing + 1] = nkey end
                end
                table.sort(members, by_name)
                if #members > 0 and #missing > 0 then
                    local seen, rel = {}, {}
                    for _, nkey in ipairs(missing) do
                        local d = nodedisp[nkey] or '(unknown table)'
                        if not seen[d] then seen[d] = true; rel[#rel + 1] = d end
                    end
                    table.sort(rel)                                  -- deterministic order
                    local relstr = table.concat(rel, ', ')
                    for _, a in ipairs(members) do
                        keep_key(a, " (FK key column - related table out of scope)",
                            "This column is part of a FOREIGN KEY key group with " .. relstr .. ", which is NOT inside the current filter. "
                            .. "Converting it alone would break the FOREIGN KEY (FOREIGN KEY and PRIMARY KEY columns must have the SAME type). "
                            .. "Re-run with a schema_pattern / table_pattern that also includes the related table(s), so the key group is converted as a whole.")
                    end
                elseif #members > 0 then
                    -- fully in scope: harmonize to the optimal common type of the member columns that hold data. A
                    -- member without data (empty table, or every value NULL; no DEFAULT) fits any type: it does not
                    -- take part in finding the type and gets the common type of the others. When no member holds
                    -- data, every member is kept as it is.
                    local data = {}
                    for _, a in ipairs(members) do
                        if not a.nodata then data[#data + 1] = a end
                    end
                    if #data == 0 then data = members end
                    -- a 0/1 member (BOOLEAN on its own) merges with its numeric facts, so a key group is never
                    -- BOOLEAN next to a number; only next to a text boolean (Y/N, TRUE/FALSE, ...) it stays BOOLEAN
                    local text_bool = false
                    for _, a in ipairs(data) do
                        if a.tgt.fam == 'BOOL' and not a.tgt.from01 then text_bool = true end
                    end
                    local function key_target(a)
                        if a.tgt.from01 and not text_bool then return a.tgt.num end
                        return a.tgt
                    end
                    local merged, why = nil, nil
                    for _, a in ipairs(data) do
                        if a.tgt.fam == 'KEEP' then
                            why = 'key column ' .. quote(a.sch) .. '.' .. quote(a.tab) .. '.' .. quote(a.col) .. ' is not converted (' .. a.conversion .. ')'
                            if a.nodata_def then
                                why = why .. '; it holds no data, but its column DEFAULT ' .. a.deftext .. ' is not checked against a common type'
                            end
                            break
                        end
                    end
                    if why == nil then
                        merged = key_target(data[1])
                        for j = 2, #data do
                            merged = merge_targets(merged, key_target(data[j]))
                            if merged.fam == 'KEEP' then why = merged.why; break end
                        end
                        if why == nil and #data == 1 then merged = merge_targets(merged, merged) end   -- applies the DOUBLE / 36-digit rules
                        if why == nil and merged.fam == 'KEEP' then why = merged.why end
                    end
                    -- a DATE member of a TIMESTAMP group must parse with the common TIMESTAMP format (full column)
                    if why == nil and merged.fam == 'TS' then
                        for _, a in ipairs(members) do
                            if a.tgt.fam == 'DATE' then
                                local okt, _, bad = count_ts_misfits({ sch = a.sch, tab = a.tab, col = a.col, defval = a.defval }, target_format(merged))
                                if not okt or bad > 0 then
                                    why = 'the values of key column ' .. quote(a.sch) .. '.' .. quote(a.tab) .. '.' .. quote(a.col) .. ' do not all parse as TIMESTAMP'
                                    break
                                end
                            end
                        end
                    end
                    if why == nil then
                        converting[root] = true
                        for _, a in ipairs(members) do
                            a.in_fk_block = true
                            if a.nodata then
                                drop_note(a, NODATA_NOTE)
                            elseif a.tgt.from01 and merged.fam == 'NUM' then
                                drop_note(a, BOOL01_NOTE)
                                for _, s in ipairs(a.tgt.num_notes) do add_note(a, s) end
                            end
                            render_change(a, merged, "  [FK key group - harmonized]")
                            if a.nodata then
                                add_note(a, "This key column holds no value (" .. a.nodata .. "), so every type fits it: it gets the common type of the other key columns of its group.")
                            elseif a.tgt.from01 and merged.fam == 'NUM' then
                                add_note(a, "Only 0/1 values: as a FOREIGN KEY key column it is converted as a number (DECIMAL), not as BOOLEAN.")
                            end
                            add_note(a, "Part of a FOREIGN KEY key group; the type is harmonized across all linked PRIMARY KEY / FOREIGN KEY columns. Run it inside the FOREIGN KEY block (DROP FOREIGN KEYS first, RE-ADD FOREIGN KEYS last).")
                        end
                    else
                        -- report the group only when it really blocks a conversion; when every member is kept
                        -- for its own reason, nothing is blocked and the rows stay normal Keep rows
                        local blocked = false
                        for _, a in ipairs(members) do
                            if a.tgt.fam ~= 'KEEP' then blocked = true end
                        end
                        if blocked then
                            for _, a in ipairs(members) do
                                keep_key(a, " (FK key group: no common convertible type - not changed)",
                                    "The PRIMARY KEY / FOREIGN KEY columns of this key group are kept identical, so the FOREIGN KEY stays valid: " .. why .. ".")
                            end
                        end
                    end
                end
            end

            -- DROP / RE-ADD for every FOREIGN KEY that touches a converted key group (composite keys per position)
            for _, key in ipairs(fkorder) do
                local d = fkdef[key]
                local touches = false
                for _, cc in ipairs(d.cols) do
                    if converting[find(nodek(d.csch, d.ctab, cc))] then touches = true; break end
                end
                if touches then
                    local ccols, rcols = {}, {}
                    for _, c in ipairs(d.cols)  do ccols[#ccols + 1] = quote(c) end
                    for _, c in ipairs(d.rcols) do rcols[#rcols + 1] = quote(c) end
                    -- reproduce the ORIGINAL state; only an explicit "disabled" maps to DISABLE, else ENABLE
                    local en    = d.enabled
                    local state = (en == false or en == 'FALSE' or en == 'false' or en == 0 or en == '0') and 'DISABLE' or 'ENABLE'
                    drop_rows[#drop_rows + 1] = { d.csch, d.ctab, '', 'drop foreign key ' .. d.cname,
                        'ALTER TABLE ' .. quote(d.csch) .. '.' .. quote(d.ctab) .. ' DROP CONSTRAINT ' .. quote(d.cname) .. ';', '' }
                    readd_rows[#readd_rows + 1] = { d.csch, d.ctab, '', 're-add foreign key ' .. d.cname .. ' (' .. state .. ')',
                        'ALTER TABLE ' .. quote(d.csch) .. '.' .. quote(d.ctab) .. ' ADD CONSTRAINT ' .. quote(d.cname) ..
                        ' FOREIGN KEY (' .. table.concat(ccols, ', ') .. ') REFERENCES ' .. quote(d.rsch) .. '.' .. quote(d.rtab) ..
                        ' (' .. table.concat(rcols, ', ') .. ') ' .. state .. ';', '' }
                end
            end
        end
    end

    if #analyzed > 0 then
        -- notes only (blank padding, '5.' / '.5' / '-0'); a Lua error adds no note and never aborts the report
        pcall(text_form_notes)
        -- a Lua error in the PRIMARY KEY check must not abort the report: no proposal that changes the values
        -- then (the column could be a PRIMARY KEY column whose texts would collide), with an ERROR row
        local okpk, errpk = pcall(pk_key_check)
        if not okpk then
            note_rows[#note_rows + 1] = { '', '', '', 'ERROR: the PRIMARY KEY check of the proposals failed', '',
                'Internal error: ' .. tostring(errpk) .. '. No proposal that changes the values (BOOLEAN, DECIMAL, DOUBLE PRECISION, DATE, TIMESTAMP, INTERVAL) is given (the column could be a PRIMARY KEY column whose texts would collide). Please report this error.' }
            for _, a in ipairs(analyzed) do
                local okx, ex = pcall(pk_cast, a)
                if not okx or ex ~= nil then
                    drop_note(a, BOOL_NOTE)
                    keep(a, " (not checked against the PRIMARY KEY - not changed)", "See the ERROR row: the PRIMARY KEY check failed.")
                end
            end
        end
        -- a Lua error in the FOREIGN KEY handling must not abort the report: an ERROR row instead
        local okfk, errfk = pcall(fk_handling)
        if not okfk then
            note_rows[#note_rows + 1] = { '', '', '', 'ERROR: the FOREIGN KEY handling failed', '',
                'Internal error: ' .. tostring(errfk) .. '. The key columns could not be harmonized, so no statement is given for any column. Please report this error.' }
            withhold_all("the FOREIGN KEY handling failed")
        end
    end

    -- Statements of all remaining (non-key) conversions; a Lua error affects only its own row
    for _, a in ipairs(analyzed) do
        if not a.in_fk_block and not a.has_change and a.tgt.fam ~= 'KEEP' then
            local okr, errr = pcall(render_change, a, a.tgt, "")
            if not okr then
                a.query_text, a.has_change = '', false
                could_not(a, "Internal error while building the statement: " .. tostring(errr))
            end
        end
    end

    ------------------------------------------------------------------------------------------------------
    -- Output: header, notes, column changes, FOREIGN KEY block, NLS restore
    ------------------------------------------------------------------------------------------------------
    local function row_of(a) return { a.sch, a.tab, a.col, a.conversion, a.query_text, table.concat(a.notes, '  ') } end

    local col_recs, key_recs, fk_note_recs = {}, {}, {}
    for _, a in ipairs(analyzed) do
        if a.in_fk_block then
            key_recs[#key_recs + 1] = a
        elseif a.fk_note then
            fk_note_recs[#fk_note_recs + 1] = a          -- key columns that are not changed (with the reason)
        elseif a.has_change or a.notice or log_all then
            col_recs[#col_recs + 1] = a
        end
    end
    table.sort(col_recs, by_name)
    table.sort(key_recs, by_name)
    table.sort(fk_note_recs, by_name)

    -- does any statement change the session NLS settings (then the last row restores them)
    local uses_nls = false
    for _, recs in ipairs({ col_recs, key_recs }) do
        for _, a in ipairs(recs) do
            if string.find(a.query_text, 'ALTER SESSION SET NLS_', 1, true) == 1 then uses_nls = true end
        end
    end
    local rollback_nls = ''
    if uses_nls then
        rollback_nls = '; in Exasol ROLLBACK also reverts ALTER SESSION, so after a ROLLBACK run the NLS restore row (the last row) again'
    end

    local sample_desc
    local auto_desc = ''
    if auto_limit ~= nil then
        auto_desc = '; tables with at most ' .. string.format('%d', auto_limit) .. ' rows are always read completely (the requested sample covers at least ' .. AUTO_MIN_PCT .. '% of each of them; on the reference cluster a full scan was not slower than a 5% sample at 250,000 and 500,000 rows)'
    end
    if sample_rows > 0 then
        -- an exact integer text, never float notation; the automatic full-scan rule is named only when it reads
        -- more tables completely than the requested number of rows already does. A number of rows of 10^15 or
        -- more is not printed (a DOUBLE is not exact there: 1e40 would print 10000000000000000303786...)
        if auto_limit ~= nil and auto_limit <= sample_rows then auto_desc = '' end
        if sample_rows >= SAMPLE_PRINT_MAX then
            sample_desc = 'sample_size of at least ' .. string.format('%.0f', SAMPLE_PRINT_MAX) .. ' rows: every table with fewer rows is read completely (in practice a full scan); every proposed conversion is verified against the full column'
        else
            local n_txt = string.format('%.0f', math.floor(sample_rows))
            sample_desc = 'random sample of about ' .. n_txt .. ' rows per table (WHERE RANDOM() < p, at most ' .. n_txt .. ' rows); tables with at most ' .. n_txt .. ' rows are read completely' .. auto_desc .. '; every proposed conversion is verified against the full column'
        end
    elseif sample_pct >= 100 then
        sample_desc = 'full scan - no sample: every row of every table (100%)'
    else
        sample_desc = 'random sample of about ' .. tostring(sample_pct) .. '% per table (WHERE RANDOM() < p, at least ' .. SAMPLE_MIN .. ' rows); smaller tables are read completely' .. auto_desc .. '; every proposed conversion is verified against the full column'
    end

    local out = {}
    out[#out + 1] = { '', '', '',
        'Analysis settings: NLS_DATE_FORMAT = ' .. sql_str(nls_date_format) .. ', NLS_TIMESTAMP_FORMAT = ' .. sql_str(nls_timestamp_format)
            .. ', NLS_NUMERIC_CHARACTERS = ' .. sql_str(nls_numeric) .. '; values read: ' .. sample_desc,
        '-- ### convert_varchar report (REPORT ONLY) - review every statement before running it ###',
        'Run each query_text cell as a whole: NLS-dependent cells start with their own ALTER SESSION, and the last row restores the NLS settings of the analysis session. Run with AUTOCOMMIT OFF: the column type changes and the FOREIGN KEY block are separate transactions, each ended by its COMMIT row; after any error run ROLLBACK (it reverts the statements since the last COMMIT). In Exasol ROLLBACK also reverts ALTER SESSION, so the NLS restore row (the last row, when present) ends with COMMIT; after a ROLLBACK run it again.' }
    for _, x in ipairs(note_rows) do out[#out + 1] = x end

    if #drop_rows > 0 and #col_recs > 0 then
        out[#out + 1] = { '', '', '', '', '-- ### COLUMN TYPE CHANGES (no FOREIGN KEY involved) ###', '' }
    end
    local n_col_changes = 0
    for _, a in ipairs(col_recs) do
        out[#out + 1] = row_of(a)
        if a.has_change then n_col_changes = n_col_changes + 1 end
    end
    if n_col_changes > 0 then
        -- ends the transaction of the column changes, so a ROLLBACK in the FOREIGN KEY block cannot revert them
        out[#out + 1] = { '', '', '', 'end of the column type changes - COMMIT only when every statement above succeeded (with AUTOCOMMIT OFF; after an error run ROLLBACK)', 'COMMIT;', '' }
    end

    -- key columns that are NOT changed (group reaches outside the filter, or no common type): no statement
    if #fk_note_recs > 0 then
        out[#out + 1] = { '', '', '', '', '-- ### FOREIGN KEY NOTES (not changed) ###', '' }
        for _, a in ipairs(fk_note_recs) do out[#out + 1] = row_of(a) end
    end

    if #drop_rows > 0 then
        out[#out + 1] = { '', '', '', 'FOREIGN KEY block: run with AUTOCOMMIT OFF; on any error ROLLBACK (restores the dropped FOREIGN KEYs and the key columns), otherwise COMMIT' .. rollback_nls,
            '-- ### FOREIGN KEY BLOCK - run the following rows as ONE transaction (AUTOCOMMIT OFF, ROLLBACK on any error) ###', '' }
        out[#out + 1] = { '', '', '', '', '-- ### DROP FOREIGN KEYS - run these FIRST (before the key column changes) ###', '' }
        for _, x in ipairs(drop_rows) do out[#out + 1] = x end
        out[#out + 1] = { '', '', '', '', '-- ### KEY COLUMN TYPE CHANGES ###', '' }
        for _, a in ipairs(key_recs) do out[#out + 1] = row_of(a) end
        out[#out + 1] = { '', '', '', '', '-- ### RE-ADD FOREIGN KEYS - run these LAST (after the key column changes) ###', '' }
        for _, x in ipairs(readd_rows) do out[#out + 1] = x end
        out[#out + 1] = { '', '', '', 'end of the FOREIGN KEY block - commit only when every statement above succeeded', 'COMMIT;', '' }
    end

    if #col_recs == 0 and #key_recs == 0 and #fk_note_recs == 0 then
        if #res == 0 then
            out[#out + 1] = { '', '', '', 'No matching VARCHAR columns found (check the schema/table filter; LIKE patterns are case-exact).', '', '' }
        else
            out[#out + 1] = { '', '', '', 'No columns found that need optimization.', '', '' }
        end
    end

    -- NLS restore row, only when some statement changes the session NLS settings. It ends with COMMIT: in
    -- Exasol ROLLBACK also reverts ALTER SESSION, so without it a later ROLLBACK in the same session would put
    -- the session back on the NLS of the last cell before the last COMMIT. Every earlier transaction has ended
    -- with its own COMMIT row, so this COMMIT only makes the session settings permanent.
    if uses_nls then
        local restore = 'ALTER SESSION SET NLS_DATE_FORMAT = ' .. sql_str(nls_date_format) .. '; ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '
                     .. sql_str(nls_timestamp_format) .. '; ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ' .. sql_str(nls_numeric) .. ';'
        if nls_date_language ~= nil then restore = restore .. ' ALTER SESSION SET NLS_DATE_LANGUAGE = ' .. sql_str(nls_date_language) .. ';' end
        restore = restore .. ' COMMIT;'
        out[#out + 1] = { '', '', '', 'Restore the session NLS settings that were active when the analysis started (run this last; it ends with COMMIT because in Exasol ROLLBACK also reverts ALTER SESSION - after a ROLLBACK run this row again)', restore, '' }
    end

    finish(out)
/

-- ====================================================================================================
-- REPORT ONLY - this prints suggestions; it does NOT change anything. REVIEW every statement before you
-- run it (numeric / date conversions can be lossy, e.g. '007' -> 7; see the notes column).
-- ====================================================================================================
EXECUTE SCRIPT DATABASE_MIGRATION.CONVERT_VARCHAR(
    'MY_SCHEMA'    -- schema_pattern: schema name or LIKE pattern (case-exact)
  , '%'            -- table_pattern: table name or LIKE pattern (case-exact)
  , '5%'           -- sample_size: rows per table (min 1000) or a percentage like '5%'; the proposals are verified
                   --              against the full columns; '100%' classifies every row; tables with at most
                   --              500,000 rows are read completely when the sample covers at least 5% of
                   --              them (on the reference cluster a full scan was not slower than a 5%
                   --              sample at 250,000 and 500,000 rows)
  , false          -- log_for_all_columns: false = changes + errors + FOREIGN KEY notes, true = every inspected column
);
