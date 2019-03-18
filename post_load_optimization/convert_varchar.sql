CREATE SCHEMA IF NOT EXISTS database_migration;

/*
    This script attempts to identify the optimal data type (and its size or precision) of VARCHAR columns
        based on the values in the given column.
    It generates the appropriate ALTER TABLE ... MODIFY COLUMN statements or advises you what steps you can
        take to convert the data type.
    No Animals Were Harmed in the Making of This Script.
*/

--/
CREATE OR REPLACE SCRIPT database_migration.convert_varchar(schema_pattern, table_pattern, sample_size) RETURNS TABLE AS

    ret = {}
    ret_type = "message VARCHAR(1000)"
    sample_rows = 0
    sample_pct  = 0
    sample_min  = 1000

    function adjust_precision(prec)
        if prec <= 9 then
            return 9
        elseif prec <= 18 then
            return 18
        else
            return 36
        end
    end

    -- Check parameters
    if schema_pattern == '' or type(schema_pattern) ~= 'string' or
       table_pattern  == '' or type(schema_pattern) ~= 'string' then
        exit(ret, ret_type)
    end

    if type(sample_size) == 'number' then
        sample_rows = math.max(math.floor(sample_size), sample_min)
    elseif type(sample_size) == 'string' then
        if string.match(sample_size, '^[0-9]+%%$') then
            sample_pct = math.max(math.min(tonumber(string.match(sample_size, '^[0-9]+')), 100), 1)
        elseif string.match(sample_size, '^[0-9]+$') then
            sample_rows = math.max(tonumber(string.match(sample_size, '^[0-9]+')), sample_min)
        else
            sample_pct = 1
        end;
    end

    -- Getting decimal character
    suc, res = pquery([[SELECT SUBSTR(session_value, 1, 1 ) FROM exa_parameters WHERE  parameter_name = 'NLS_NUMERIC_CHARACTERS']])
    dec_char = res[1][1]
    regexp_int = '^ *[+-]?[0-9]+ *$'
    regexp_dec = '^ *[+-]?[0-9]*\\'  .. dec_char .. '[0-9]+ *$'
    regexp_dpr = '^ *[+-]?[0-9]*(\\' .. dec_char .. '[0-9]+)?[eE][+-]?[0-9]+ *$'

    -- Getting NLS_DATE_FORMAT
    suc, res = pquery([[SELECT session_value FROM exa_parameters WHERE parameter_name = 'NLS_DATE_FORMAT']])
    nls_date_format = res[1][1]

    -- Getting NLS_TIMESTAMP_FORMAT
    suc, res = pquery([[SELECT session_value FROM exa_parameters WHERE parameter_name = 'NLS_TIMESTAMP_FORMAT']])
    nls_timestamp_format = res[1][1]

    -- Ensuring that timestamp arithmetic is interval to avoid error thrown when comparing
    -- timestamp and date difference to interval
    pquery([[ALTER SESSION SET TIMESTAMP_ARITHMETIC_BEHAVIOR = 'INTERVAL']])

    -- Getting columns
    suc, res = pquery([[
        SELECT column_schema
             , column_table
             , column_name
             , column_maxsize
        FROM   exa_all_columns
        WHERE  column_schema LIKE :schp
           AND column_table  LIKE :tabp
           AND column_type_id = 12  -- VARCHAR (Should CHAR be considered, too?)
        ORDER  BY column_schema
                , column_table
                , column_ordinal_position
     ]], {
         schp = schema_pattern
       , tabp = table_pattern
     })

    tab_name   = "-"
    tab_rows   = 0;
    tab_sample = 0;

    -- Iterating through tables and their columns
    for i = 1, #res do
        curr_sch = res[i]["COLUMN_SCHEMA"]
        curr_tab = res[i]["COLUMN_TABLE"]
        curr_col = res[i]["COLUMN_NAME"]
        curr_col_len = res[i]["COLUMN_MAXSIZE"]
        if tab_name ~= curr_tab then
            tab_name = curr_tab
            suc, res2 = pquery([[
                            SELECT COUNT(*) FROM ::sch.::tab
                        ]], {
                            sch = quote(curr_sch)
                          , tab = quote(curr_tab)
                        })
            tab_rows = res2[1][1]
            msg = "-- Table " .. curr_tab .. " (" .. tab_rows .. " rows; sample size is "
            if sample_rows ~= 0 then
                tab_sample = sample_rows
                msg = msg .. tab_sample .. " rows)"
            else
                tab_sample = math.floor(tab_rows * (sample_pct / 100))
                if tab_sample < sample_min then
                    tab_sample = math.min(tab_rows, sample_min)
                    msg = msg .. tab_sample .. " rows)"
                else
                    msg = msg .. tab_sample .. " rows, " .. sample_pct .. "%)"
                end
            end
            ret[#ret + 1] = {msg}
        end
        ret[#ret + 1] = {"   -- Column " .. curr_col}

        suc, res2 = pquery([[
            SELECT SUM(CASE WHEN col IS NOT NULL THEN 1 ELSE 0 END) AS entries
                 , SUM(CASE WHEN IS_NUMBER(col) AND col REGEXP_LIKE :r_int
                            THEN 1
                            ELSE 0
                       END) its_integer
                 , MAX(CASE WHEN IS_NUMBER(col) AND col REGEXP_LIKE :r_int
                            THEN LENGTH(LTRIM(TRIM(col), '+-'))
                            ELSE 0
                       END) its_integer_precision
                 , SUM(CASE WHEN IS_NUMBER(col) AND col REGEXP_LIKE :r_dec
                            THEN 1
                            ELSE 0
                       END) its_decimal
                 , MAX(CASE WHEN IS_NUMBER(col) AND col REGEXP_LIKE :r_dec
                            THEN LENGTH(LTRIM(TRIM(SUBSTR(col, 1, INSTR(col, '.') - 1), '+-')))
                            ELSE 0
                       END) its_decimal_precision
                 , MAX(CASE WHEN IS_NUMBER(col) AND col REGEXP_LIKE :r_dec
                            THEN LENGTH(LTRIM(TRIM(SUBSTR(col, INSTR(col, '.') + 1), '+-')))
                            ELSE 0
                       END) its_decimal_scale
                 , SUM(CASE WHEN IS_NUMBER(col) AND col REGEXP_LIKE :r_dpr
                            THEN 1
                            ELSE 0
                       END) its_double_precision
                 , SUM(CASE WHEN IS_DATE(col) AND TO_TIMESTAMP(col) - TO_DATE(col) = INTERVAL '00:00:00.000000' HOUR TO SECOND
                            THEN 1
                            ELSE 0
                       END) its_date
                 , SUM(CASE WHEN IS_TIMESTAMP(col) AND TO_TIMESTAMP(col) - TO_DATE(col) <> INTERVAL '00:00:00.000000' HOUR TO SECOND
                            THEN 1
                            ELSE 0
                       END) its_timestamp
                 , SUM(CASE WHEN IS_BOOLEAN(col)    THEN 1 ELSE 0 END) its_boolean
                 , SUM(CASE WHEN col IN ('0', '1')  THEN 1 ELSE 0 END) its_boolean_binary
                 , SUM(CASE WHEN IS_DSINTERVAL(col) THEN 1 ELSE 0 END) its_dsinterval
                 , MAX(CASE WHEN IS_DSINTERVAL(col)
                            THEN LENGTH(LTRIM(SUBSTR(col, 1, INSTR(col, ' ') - 1), '+-'))
                            ELSE 0
                       END) its_dsinterval_p
                 , MAX(CASE WHEN IS_DSINTERVAL(col)
                            THEN LENGTH(SUBSTR(col, INSTR(col, '.') + 1))
                            ELSE 0
                       END) its_dsinterval_fp
                 , SUM(CASE WHEN IS_YMINTERVAL(col) THEN 1 ELSE 0 END) its_yminterval
                 , MAX(CASE WHEN IS_YMINTERVAL(col)
                            THEN LENGTH(LTRIM(SUBSTR(col, 1, INSTR(col, '-') - 1), '+-'))
                            ELSE 0
                       END) its_yminterval_p
                 , SUM(CASE WHEN UPPER(col) REGEXP_LIKE '.*POINT *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*LINESTRING *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*LINEARRING *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*POLYGON *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*GEOMETRYCOLLECTION *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*MULTIPOINT *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*MULTILINESTRING *\(.*'
                              OR UPPER(col) REGEXP_LIKE '.*MULTIPOLYGON *\(.*'
                            THEN 1
                            ELSE 0
                       END) its_geometry
                 , MAX(LENGTH(col)) max_length
            FROM   (
                       SELECT ::col AS col
                       FROM   ::sch.::tab
                       LIMIT  :lmt
                   )
        ]], {
            sch = quote(curr_sch)
          , tab = quote(curr_tab)
          , col = quote(curr_col)
          , lmt = tab_sample
          , r_int = regexp_int
          , r_dec = regexp_dec
          , r_dpr = regexp_dpr
        })

        new_type  = "-"
        ucol = string.upper(curr_col)

        -- Let's try to figure out what would be the best data type
        res3 = res2[1]
        if res3["ENTRIES"] == 0 then -- No data in the column; can't decide data type
            ret[#ret + 1] = {"      -- No data in column, can't identify alternative data type. Maybe column could be dropped?"}
            ret[#ret + 1] = {"      -- ALTER TABLE " .. quote(curr_sch) .. "." .. quote(curr_tab) .. " DROP COLUMN " .. quote(curr_col) .. ";"}
        elseif res3["ENTRIES"] == res3["ITS_INTEGER"] then -- All sample values were integers
            -- But it could be a binary (0/1) boolean column...
            if res3["ENTRIES"] == res3["ITS_BOOLEAN_BINARY"] then
                new_type = "BOOLEAN"
            else
                if res3["ITS_INTEGER_PRECISION"] > 36 then
                    ret[#ret + 1] = {"      -- WARNING: Largest decimal precision found was " .. res3["ITS_INTEGER_PRECISION"] .. "; larger than maximum 36. Data type conversion is not possible"}
                else
                    new_type = "DECIMAL(" .. adjust_precision(res3["ITS_INTEGER_PRECISION"]) .. ")"
                end
            end
        elseif res3["ENTRIES"] == res3["ITS_INTEGER"] + res3["ITS_DECIMAL"] then -- All sample values were integers and floats/decimals (but not in scientific notation)
            total_precision = math.max(res3["ITS_INTEGER_PRECISION"], res3["ITS_DECIMAL_PRECISION"]) + res3["ITS_DECIMAL_SCALE"]
            if total_precision > 36 then
                ret[#ret + 1] = {"      -- WARNING: Largest decimal precision found was " .. total_precision .. "; larger than maximum 36. Data type conversion is not possible"}
            else
                new_type = "DECIMAL(" .. total_precision .. ", " .. res3["ITS_DECIMAL_SCALE"] .. ")"
            end
        elseif res3["ENTRIES"] == res3["ITS_INTEGER"] + res3["ITS_DECIMAL"] + res3["ITS_DOUBLE_PRECISION"] then -- All sample values were some kind of numerical values
            new_type = "DOUBLE PRECISION"
        elseif res3["ENTRIES"] == res3["ITS_DATE"] then -- All sample values were dates (but not timestamps)
            new_type = "DATE"
        elseif res3["ENTRIES"] == res3["ITS_DATE"] + res3["ITS_TIMESTAMP"] then -- All sample values were dates or timestamps
            ret[#ret + 1] = {"      -- Consider if TIMESTAMP WITH LOCAL TIME ZONE was a more appropriate data type."}
            new_type = "TIMESTAMP"
        elseif res3["ENTRIES"] == res3["ITS_BOOLEAN"] then -- All sample values were Boolean
            new_type = "BOOLEAN"
        elseif res3["ENTRIES"] == res3["ITS_DSINTERVAL"] then -- All sample values were day-to-second intervals
            new_type = "INTERVAL DAY(" .. math.min(math.max(res3["ITS_DSINTERVAL_P"], 1), 9) .. ") TO SECOND(" .. math.min(res3["ITS_DSINTERVAL_FP"], 9) .. ")"
        elseif res3["ENTRIES"] == res3["ITS_YMINTERVAL"] then -- All sample values were year-to-months intervals
            new_type = "INTERVAL YEAR(" .. math.min(math.max(res3["ITS_YMINTERVAL_P"], 1), 9) ..") TO MONTH"
        elseif res3["ENTRIES"] == res3["ITS_GEOMETRY"] then -- All sample values seem to contain geospatial data
            ret[#ret + 1] = {"      -- You might want to specify SRID (a reference coordinate system, query EXA_SPATIAL_REF_SYS for possible values)"}
            new_type = "GEOMETRY"
        elseif string.match(ucol, "_TIMESTAMP") or string.match(ucol, "TIMESTAMP$")or
               string.match(ucol, "_TS") or string.match(ucol, "TS$") or
               string.match(ucol, "_DATETIME") or string.match(ucol, "DATETIME$") or
               ucol == "TIMSTAMP" then -- Maybe timestamp column
            ret[#ret + 1] = {"      -- This might be a timestamp column but the values do not match current timestamp format (" .. nls_timestamp_format .. ")"}
            ret[#ret + 1] = {"      -- UPDATE " .. quote(curr_sch) .. "." .. quote(curr_tab) .. " SET " .. quote(curr_col) .. " = TO_CHAR(TO_TIMESTAMP(" .. quote(curr_col) .. ", '<timestamp_format>'), '" .. nls_timestamp_format .. "');"}
            ret[#ret + 1] = {"      -- ALTER TABLE " .. quote(curr_sch) .. "." .. quote(curr_tab) .. " MODIFY COLUMN " .. quote(curr_col) .. " TIMESTAMP [WITH LOCAL TIME ZONE];"}
        elseif string.match(ucol, "_DATE") or string.match(ucol, "DATE$") or string.match(ucol, "^DATE_") or
               string.match(ucol, "_DT") or string.match(ucol, "DT$") or
               ucol == "DATE" or ucol == 'DOB' then -- Maybe date column
            ret[#ret + 1] = {"      -- This might be a date column but the values do not match current date format (" .. nls_date_format .. ")"}
            ret[#ret + 1] = {"      -- UPDATE " .. quote(curr_sch) .. "." .. quote(curr_tab) .. " SET " .. quote(curr_col) .. " = TO_CHAR(TO_DATE(" .. quote(curr_col) .. ", '<date_format>'), '" .. nls_date_format .. "');"}
            ret[#ret + 1] = {"      -- ALTER TABLE " .. quote(curr_sch) .. "." .. quote(curr_tab) .. " MODIFY COLUMN " .. quote(curr_col) .. " DATE;"}
        elseif res3["MAX_LENGTH"] < curr_col_len then -- Mixed/unclear types of values, only shrink column width (if possible)
            new_col_len = res3["MAX_LENGTH"]
            --new_col_len = math.min(math.floor(res3["MAX_LENGTH"] * 1.2), 2000000) -- Not sure if we need to be prepared for even larger strings
            if new_col_len < curr_col_len then
                ret[#ret + 1] = {"      -- Mixed values in column, retaining existing data type but shrinking maximum column length"}
                new_type = "VARCHAR(" .. new_col_len .. ")"
            end
        else
            ret[#ret + 1] = {"      -- Mixed values in column, retaining existing data type and size"}
        end;

        if new_type ~= "-" then
            ret[#ret + 1] = {"      ALTER TABLE " .. quote(curr_sch) .. "." .. quote(curr_tab) .. " MODIFY COLUMN " .. quote(curr_col) .. " " .. new_type .. ";"}
        end

    end

    exit(ret, ret_type)
/

EXECUTE SCRIPT database_migration.convert_varchar(
    'MY_SCHEMA'    -- schema_pattern: schema name or schema name filter (can be %)
  , '%'            -- table_pattern: table name or table name filter (can be %)
  , '5%'           -- sample_size: table data sample size; either an integer value or a string expressing a integer percentage
);

-- EOF