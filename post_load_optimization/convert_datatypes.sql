create schema if not exists database_migration;
/*
    This script creates datatype optimizations for you. You can run this after
    importing your data. Selecting smaller datatypes might improve performance.

    This script:
  - looks at all columns of type 'DOUBLE' and converts them directly to the
    smallest fitting DECIMAL if the values are exactly representable: integer values
    -> DECIMAL(p,0), or values with a small constant number of decimals (e.g. prices)
    -> DECIMAL(p,s). The conversion is only proposed when a round-trip cast proves it
    is lossless for every value; genuine floating-point values stay DOUBLE.
  - looks at all columns of type 'DECIMAL' (scale = 0) and converts them to a
    smaller integer type (32-bit / 64-bit) if a smaller datatype is sufficient.
  - looks at all columns of type 'DECIMAL' (scale <> 0) and converts them to a
    smaller decimal if a smaller datatype is sufficient (scale is preserved).
  - looks at all columns of type 'TIMESTAMP' and 'TIMESTAMP WITH LOCAL TIME ZONE'
    and converts them to DATE if only date values (no time component) are contained
    in the column. For TIMESTAMP WITH LOCAL TIME ZONE the check and the conversion are
    evaluated in the current SESSIONTIMEZONE (both in the same session, so consistent).
  - looks at all columns of type 'VARCHAR' and converts them to a smaller
    VARCHAR if a smaller VARCHAR can still hold the information in the column.
    The original character set (ASCII / UTF8) is always preserved.

    Each of the five conversion types above can be switched on/off individually via the
    convert_double / convert_integer / convert_decimal / convert_timestamp / convert_varchar
    parameters (true = check & possibly convert this type, false = skip it entirely).

    Only real, local base TABLE columns are inspected:
      * views and synonyms are excluded (COLUMN_OBJECT_TYPE = 'TABLE')
      * VIRTUAL SCHEMA columns are excluded (COLUMN_IS_VIRTUAL = FALSE)

    ====================================================================================
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  A T T E N T I O N  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ====================================================================================
    The parameter apply_conversion = TRUE will IRREVERSIBLY change your table
    definitions with ALTER TABLE ... MODIFY statements.

      -> ONLY use apply_conversion = TRUE when you have reviewed the dry-run output
         (apply_conversion = FALSE) and are 100% sure that EVERY proposed conversion
         must really be performed.
      -> The SAFER way is to run with apply_conversion = FALSE first, then copy the
         generated statements from the "query_text" column and execute them YOURSELF,
         one statement at a time, checking each result.
      -> A conversion that does not fit the data will fail at execution time; with
         apply_conversion = TRUE the error only shows up in the "success" column.

    Always have a backup / be able to recreate the affected tables before applying.
    ====================================================================================

    Case sensitivity:
      All schema/table/column names are processed as DELIMITED (double-quoted)
      identifiers via quote() and the ::identifier placeholder, so MixedCase and
      lowercase names work correctly. The schema_name / table_name FILTER is a VALUE
      comparison against the (case-exact) catalog names, so pass the name exactly as
      it is stored, or use '%'.
*/

--parameter	schema_name: 	  SCHEMA name or SCHEMA_FILTER (can be %)
--parameter	table_name: 	  TABLE name or TABLE_FILTER  (can be %)
--parameter	convert_double:    true/false - check/convert DOUBLE      -> smallest fitting DECIMAL(p,0) / DECIMAL(p,s)
--parameter	convert_integer:   true/false - check/convert DECIMAL(p,0) -> DECIMAL(9,0) / DECIMAL(18,0)
--parameter	convert_decimal:   true/false - check/convert DECIMAL(p,s) -> DECIMAL(9,s) / DECIMAL(18,s)
--parameter	convert_timestamp: true/false - check/convert TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE -> DATE
--parameter	convert_varchar:   true/false - check/convert VARCHAR(n)   -> smaller VARCHAR (same charset)
--parameter	log_for_all_columns: true/false - false = report only columns that change; true = report every inspected column (incl. 'Keep ...' rows)
--parameter	apply_conversion: !!! ATTENTION !!! true or false. TRUE irreversibly alters your tables - only use it when you are 100% sure after reviewing the dry-run (false). The safer way is to review the output of a false-run and execute the statements manually, one by one.
--/
create or replace script database_migration.convert_datatypes(schema_name, table_name, convert_double, convert_integer, convert_decimal, convert_timestamp, convert_varchar, log_for_all_columns, apply_conversion) RETURNS TABLE
 as

------------------------------------------------------------------------------------------------------
-- Returns the smaller integer precision (9 = 32-bit, 18 = 64-bit) that still fits
-- "needed" digits, or nil if no smaller type than current_precision is possible.
------------------------------------------------------------------------------------------------------
function smaller_integer_precision(needed, current_precision)
	if needed <= 9 and current_precision > 9 then
		return 9     -- fits into 32-bit
	elseif needed <= 18 and current_precision > 18 then
		return 18    -- fits into 64-bit
	else
		return nil   -- no smaller type fits
	end
end

------------------------------------------------------------------------------------------------------
-- For a DOUBLE column that also contains fractional values, find the smallest scale s
-- (1..MAX_DOUBLE_SCALE) at which EVERY non-null value is exactly representable as a
-- DECIMAL of that scale, proven per row by a round-trip cast:
--    cast(cast(value as decimal(36,s)) as double) = value
-- i.e. the DECIMAL reproduces the original DOUBLE bit-for-bit (lossless). It also requires
-- the resulting precision (integer digits + s) to fit into 18 (64-bit), so the DECIMAL is
-- never larger than the 8-byte DOUBLE. Returns scale, max_int_digits -- or nil if none.
------------------------------------------------------------------------------------------------------
function detect_lossless_double_scale(scm, tbl, col)
	local MAX_DOUBLE_SCALE = 9    -- conservative cap (prices, rates, ...); higher-scale data stays DOUBLE
	local viol = ''
	for s = 0, MAX_DOUBLE_SCALE do
		viol = viol .. ", count(case when cast(cast(::col_name as decimal(36,"..s..")) as double) <> ::col_name then 1 end)"
	end
	local ok, r = pquery([[
		select coalesce(max(length(cast(floor(abs(::col_name)) as decimal(36,0)))), 0) ]] .. viol .. [[
		from ::curr_schema.::curr_table
	]], {curr_schema=scm, curr_table=tbl, col_name=col})
	if not ok then
		return nil   -- e.g. a value too large to cast -> keep DOUBLE
	end

	local max_int_digits = r[1][1]
	-- r[1][2] = round-trip violations at scale 0 ; r[1][2+s] = violations at scale s
	if r[1][2] == 0 then
		return nil   -- all values are integer-valued: handled by the integer path, don't invent a scale
	end
	for s = 1, MAX_DOUBLE_SCALE do
		if r[1][2 + s] == 0 and (max_int_digits + s) <= 18 then
			return s, max_int_digits
		end
	end
	return nil
end

------------------------------------------------------------------------------------------------------
-- DOUBLE --> DECIMAL : convert DOUBLE columns that contain only integer values, or only
-- values that are exactly representable as a small-scale DECIMAL, directly to the smallest
-- fitting DECIMAL (one single, lossless ALTER statement).
------------------------------------------------------------------------------------------------------
function convert_double_to_decimal(schema_name, table_name, log_for_all_columns)

	local result_table = {}
	local res = query([[
			select
				column_schema,
				column_table,
				column_name
			from
				exa_all_columns
			where
				column_type_id = 8 and             -- type_id of DOUBLE
				column_schema like :schema_filter and
				column_table  like :table_filter and
				column_object_type = 'TABLE' and   -- only base tables, never views/synonyms
				column_is_virtual  = FALSE         -- never virtual schema columns
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column    = false
		local message_action   = ''
		local query_to_execute = ''

		local scm = quote(res[i][1])
		local tbl = quote(res[i][2])
		local col = quote(res[i][3])

		-- Single table scan: not-null count, number of rows that are NOT a plain
		-- integer, and the max number of integer digits (only meaningful when all
		-- values are integers). pquery: if a value cannot be cast to DECIMAL at all
		-- (overflow) we keep the column instead of aborting the whole script.
		local tsSuc, tsColumns = pquery([[
			select
				count(::col_name) as values_in_column,
				count(case when
					not(
						abs(::col_name - cast(::col_name as decimal)) < 0.00000000001
							and
						(
							to_char(::col_name) = '0'
								or
							abs(::col_name) >= 1
						)
							and
						abs(::col_name) < 1E14
					)
				then 1 end) as non_integer_rows,
				coalesce(max(length(cast(abs(::col_name) as decimal(18,0)))), 0) as max_int_length
			from
				::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		-- tsColumns[1][1] = number of not-null values
		-- tsColumns[1][2] = number of rows that are NOT plain integers
		-- tsColumns[1][3] = max number of integer digits
		if not tsSuc then
			message_action = 'Keep DOUBLE (too big for DECIMAL)'

		elseif tsColumns[1][1] == 0 then
			message_action = 'Keep DOUBLE (IS EMPTY)'

		elseif tsColumns[1][2] == 0 then
			-- integer only -> convert directly to the smallest fitting DECIMAL.
			-- (values are < 1E14, so they always fit into DECIMAL(18,0))
			local max_int_length = tsColumns[1][3]
			local target
			if max_int_length <= 9 then
				target = 9    -- fits into 32-bit
			else
				target = 18   -- fits into 64-bit
			end
			query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..",0));"
			modify_column    = true
			message_action   = 'DOUBLE --> DECIMAL('..target..', 0), max length: '..max_int_length

		else
			-- contains fractional values -> only convert if a DECIMAL(p,s) can represent
			-- EVERY value losslessly (proven by a round-trip cast). Otherwise keep DOUBLE.
			local s, max_int_digits = detect_lossless_double_scale(scm, tbl, col)
			if s ~= nil then
				local p = max_int_digits + s
				local target
				if p <= 9 then
					target = 9    -- fits into 32-bit
				else
					target = 18   -- fits into 64-bit
				end
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..","..s.."));"
				modify_column    = true
				message_action   = 'DOUBLE --> DECIMAL('..target..', '..s..') (lossless)'
			else
				message_action = 'Keep DOUBLE'
			end
		end

		if modify_column or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3], message_action, query_to_execute}
		end
	end
	return result_table
end -- convert_double_to_decimal

------------------------------------------------------------------------------------------------------
-- DECIMAL(p,0) --> smaller integer DECIMAL
------------------------------------------------------------------------------------------------------
function convert_integer_to_smaller_integer(schema_name, table_name, log_for_all_columns)

	local result_table = {}
	local res = query([[
			select
				column_schema,
				column_table,
				column_name,
				column_num_prec
			from
				exa_all_columns
			where
				column_type_id = 3 and             -- type_id of DECIMAL
				column_schema like :schema_filter and
				column_table  like :table_filter and
				column_object_type = 'TABLE' and
				column_is_virtual  = FALSE and
				column_num_scale = 0 and           -- only columns without scale
				column_num_prec  > 9
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column    = false
		local message_action   = ''
		local query_to_execute = ''

		local scm       = quote(res[i][1])
		local tbl       = quote(res[i][2])
		local col       = quote(res[i][3])
		local precision = res[i][4]

		-- Single table scan: not-null count + max number of digits.
		local dColumns = query([[
			select
				count(::col_name) as values_in_column,
				coalesce(max(length(abs(::col_name))), 0) as max_length
			from
				::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		local not_null_count = dColumns[1][1]
		local max_length     = dColumns[1][2]

		if not_null_count == 0 then
			message_action = 'Keep DECIMAL (IS EMPTY)'
		else
			local target = smaller_integer_precision(max_length, precision)
			if target ~= nil then
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..",0));"
				modify_column    = true
				message_action   = 'DECIMAL('..precision..', 0)  --> DECIMAL('..target..', 0), max length: '..max_length
			else
				message_action = 'Keep DECIMAL('..precision..', 0), max length: '..max_length
			end
		end

		if modify_column or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3], message_action, query_to_execute}
		end
	end
	return result_table
end -- convert_integer_to_smaller_integer

------------------------------------------------------------------------------------------------------
-- DECIMAL(p,s) --> smaller DECIMAL (scale preserved)
------------------------------------------------------------------------------------------------------
function convert_decimal_with_scale_to_smaller_decimal(schema_name, table_name, log_for_all_columns)

	local result_table = {}
	local res = query([[
			select
				column_schema,
				column_table,
				column_name,
				column_num_prec,
				column_num_scale
			from
				exa_all_columns
			where
				column_type_id = 3 and             -- type_id of DECIMAL
				column_schema like :schema_filter and
				column_table  like :table_filter and
				column_object_type = 'TABLE' and
				column_is_virtual  = FALSE and
				column_num_scale <> 0 and          -- only columns that have a scale
				column_num_prec   > 9
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column    = false
		local message_action   = ''
		local query_to_execute = ''

		local scm       = quote(res[i][1])
		local tbl       = quote(res[i][2])
		local col       = quote(res[i][3])
		local precision = res[i][4]
		local col_scale = res[i][5]

		-- Single table scan: not-null count + max length of the integer part.
		-- floor() (not round()) so a value like 999.99 counts as 3 integer digits,
		-- not 4 -- otherwise an otherwise-possible shrink would be missed.
		local dColumns = query([[
			select
				count(::col_name) as values_in_column,
				coalesce(max(length(floor(abs(::col_name)))), 0) as max_int_length
			from
				::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		local not_null_count   = dColumns[1][1]
		local max_int_length   = dColumns[1][2]
		local needed_precision = max_int_length + col_scale

		if not_null_count == 0 then
			message_action = 'Keep DECIMAL (IS EMPTY)'
		else
			-- needed_precision always includes the scale, so the target precision is
			-- always > scale -> DECIMAL(target, scale) is guaranteed to be valid.
			local target = smaller_integer_precision(needed_precision, precision)
			if target ~= nil then
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..","..col_scale.."));"
				modify_column    = true
				message_action   = 'DECIMAL('..precision..', '..col_scale..')  --> DECIMAL('..target..', '..col_scale..'), needed precision: '..needed_precision
			else
				message_action = 'Keep DECIMAL('..precision..', '..col_scale..'), needed precision: '..needed_precision
			end
		end

		if modify_column or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3], message_action, query_to_execute}
		end
	end
	return result_table
end -- convert_decimal_with_scale_to_smaller_decimal

------------------------------------------------------------------------------------------------------
-- TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE --> DATE
-- Convert timestamp columns that never carry a time component.
------------------------------------------------------------------------------------------------------
function convert_timestamp_to_date(schema_name, table_name, log_for_all_columns)

	local result_table = {}
	local res = query([[
			select
				column_schema,
				column_table,
				column_name,
				column_type_id
			from
				exa_all_columns
			where
				column_type_id in (93, 124) and    -- 93 = TIMESTAMP, 124 = TIMESTAMP WITH LOCAL TIME ZONE
				column_schema like :schema_filter and
				column_table  like :table_filter and
				column_object_type = 'TABLE' and   -- only base tables, never views/synonyms
				column_is_virtual  = FALSE         -- never virtual schema columns
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column    = false
		local message_action   = ''
		local query_to_execute = ''

		local scm = quote(res[i][1])
		local tbl = quote(res[i][2])
		local col = quote(res[i][3])

		-- label the source type for the output messages
		local type_label = 'TIMESTAMP'
		if res[i][4] == 124 then
			type_label = 'TIMESTAMP WITH LOCAL TIME ZONE'
		end

		-- Single table scan: not-null count + number of rows that carry a time
		-- component. We use TRUNC() instead of subtracting datetimes, so the check
		-- does NOT depend on the TIMESTAMP_ARITHMETIC_BEHAVIOR parameter (which
		-- decides whether datetime subtraction yields an INTERVAL or a DOUBLE).
		-- For TIMESTAMP WITH LOCAL TIME ZONE both the TRUNC check and the later DATE
		-- conversion run in the current SESSIONTIMEZONE within the same session, so
		-- they are consistent.
		-- pquery so a problem skips the column instead of aborting the run.
		local tsSuc, tsColumns = pquery([[
			select
				count(::col_name) as values_in_column,
				count(case when ::col_name <> TRUNC(::col_name) then 1 end) as rows_with_time
			from
				::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		-- tsColumns[1][1] = number of not-null values
		-- tsColumns[1][2] = number of rows that carry a time component
		if not tsSuc then
			message_action = 'Keep '..type_label..' (check failed)'

		elseif tsColumns[1][1] == 0 then
			message_action = 'Keep '..type_label..' (IS EMPTY)'

		elseif tsColumns[1][2] == 0 then
			-- date only (no hours, minutes, seconds, fraction)
			query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DATE);"
			modify_column    = true
			message_action   = type_label..' --> DATE'

		else
			message_action = 'Keep '..type_label
		end

		if modify_column or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3], message_action, query_to_execute}
		end
	end
	return result_table
end -- convert_timestamp_to_date

------------------------------------------------------------------------------------------------------
-- VARCHAR --> smaller VARCHAR
------------------------------------------------------------------------------------------------------
function convert_varchar_to_smaller_varchar(schema_name, table_name, log_for_all_columns)

	local result_table = {}
	local res = query([[
			select
				column_schema,
				column_table,
				column_name,
				column_maxsize,
				column_type
			from
				exa_all_columns
			where
				column_type_id = 12 and            -- type_id of VARCHAR
				column_schema like :schema_filter and
				column_table  like :table_filter and
				column_object_type = 'TABLE' and
				column_is_virtual  = FALSE and
				column_maxsize > 3                 -- do not modify columns of size <= 3
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column    = false
		local message_action   = ''
		local query_to_execute = ''

		local scm             = quote(res[i][1])
		local tbl             = quote(res[i][2])
		local col             = quote(res[i][3])
		local current_maxsize = res[i][4]

		-- Preserve the existing character set. COLUMN_TYPE looks like
		-- "VARCHAR(2000000) ASCII" or "VARCHAR(2000000) UTF8". If the charset is
		-- omitted in MODIFY, Exasol falls back to the UTF8 default and would silently
		-- turn an ASCII column into UTF8 -- so we ALWAYS re-state the original charset.
		local charset = 'UTF8'
		if string.find(string.upper(res[i][5]), 'ASCII', 1, true) ~= nil then
			charset = 'ASCII'
		end

		-- Single table scan: not-null count + actual max character length.
		local dColumns = query([[
			select
				count(::col_name) as values_in_column,
				coalesce(max(length(::col_name)), 0) as max_length
			from
				::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		local not_null_count = dColumns[1][1]
		local max_length     = dColumns[1][2]

		if not_null_count == 0 then
			message_action = 'Keep VARCHAR('..current_maxsize..') '..charset..' (IS EMPTY)'

		elseif (max_length <= 2000000) and (estimate_optimal_varchar_length(max_length) < current_maxsize) then
			-- a smaller varchar fits and is still <= 2,000,000 (character set is preserved)
			local change_to  = estimate_optimal_varchar_length(max_length)  -- actual length + a bit of buffer
			query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." VARCHAR("..change_to..") "..charset..");"
			modify_column    = true
			message_action   = 'VARCHAR('..current_maxsize..') '..charset..'  --> VARCHAR('..change_to..') '..charset..', max length: '..max_length

		else
			message_action = 'Keep VARCHAR('..current_maxsize..') '..charset..', max length: '..max_length
		end

		if modify_column or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3], message_action, query_to_execute}
		end
	end
	return result_table
end -- convert_varchar_to_smaller_varchar

------------------------------------------------------------------------------------------------------
-- Helper functions
------------------------------------------------------------------------------------------------------

-- Takes a number as input, adds 20% headroom and rounds up to the next bigger round decimal.
function estimate_optimal_varchar_length(n)
	local n_p20            = math.ceil(n + n * 0.2)               -- add 20% so everything fits in
	local number_digits    = string.len(n_p20)
	local magnitude        = math.floor(10 ^ (number_digits - 1))
	local estimated_length = math.floor(n_p20 / magnitude) * magnitude + magnitude
	return math.min(estimated_length, 2000000)                   -- maximal varchar size is 2,000,000
end

-- Appends all rows of t2 to t1 and returns t1.
function merge_tables(t1, t2)
	for i = 1, #t2 do
		t1[#t1+1] = t2[i]
	end
	return t1
end

-- Returns the maximal string length found in one column of the result table.
function getMaxLengthForColumn(input_table, column_number)
	local length = 1
	for i = 1, #input_table do
		if #input_table[i][column_number] > length then
			length = #input_table[i][column_number]
		end
	end
	return length
end

-- Executes the SQL contained in one column and logs the outcome in another column.
function execute_sql_column(input_table, sql_col_number, log_col_number)
	for i = 1, #input_table do
		local sql_stmt = input_table[i][sql_col_number]
		if sql_stmt ~= nil and sql_stmt ~= '' then
			local sql_suc, sql_res = pquery(sql_stmt)
			if sql_suc then
				input_table[i][log_col_number] = 'true'
			else
				input_table[i][log_col_number] = 'ERROR: ' .. sql_res.error_message
			end
		else
			input_table[i][log_col_number] = ''   -- nothing to execute (e.g. a "Keep" row)
		end
	end
	return input_table
end

-----------------------------END OF FUNCTIONS, BEGINNING OF ACTUAL SCRIPT-----------------------------

	-- log_for_all_columns is a script parameter: if false, only columns that will be changed
	-- are reported; if true, every inspected column is reported (including "Keep ..." rows).

	local overall_res = {}

	-- Each conversion type runs only when its switch parameter is true.
	-- double to decimal (directly to the smallest fitting DECIMAL)
	if convert_double then
		overall_res = merge_tables(overall_res, convert_double_to_decimal(schema_name, table_name, log_for_all_columns))
	end

	-- integer (scale 0) to smaller integer
	if convert_integer then
		overall_res = merge_tables(overall_res, convert_integer_to_smaller_integer(schema_name, table_name, log_for_all_columns))
	end

	-- decimal with scale to smaller decimal
	if convert_decimal then
		overall_res = merge_tables(overall_res, convert_decimal_with_scale_to_smaller_decimal(schema_name, table_name, log_for_all_columns))
	end

	-- timestamp / timestamp with local time zone to date
	if convert_timestamp then
		overall_res = merge_tables(overall_res, convert_timestamp_to_date(schema_name, table_name, log_for_all_columns))
	end

	-- varchar to smaller varchar
	if convert_varchar then
		overall_res = merge_tables(overall_res, convert_varchar_to_smaller_varchar(schema_name, table_name, log_for_all_columns))
	end

	-- execute the collected statements if apply_conversion is true
	-- !!! ATTENTION: this irreversibly alters your tables - see the header banner. !!!
	if apply_conversion then
		overall_res = execute_sql_column(overall_res, 5, 6)
	end

	-- sort the whole output by schema_name, table_name, column_name
	table.sort(overall_res, function(a, b)
		if a[1] ~= b[1] then return a[1] < b[1] end
		if a[2] ~= b[2] then return a[2] < b[2] end
		return a[3] < b[3]
	end)

	-- friendly message when the result set would be empty.
	-- With log_for_all_columns = true every inspected column is listed, so an empty result
	-- means nothing matched the filter / enabled types at all; otherwise it means that no
	-- column needs optimization.
	if #overall_res == 0 then
		local empty_msg
		if log_for_all_columns then
			empty_msg = 'No matching columns found (check the filters and the convert_* switches).'
		else
			empty_msg = 'No columns found that need optimization.'
		end
		if apply_conversion then
			overall_res[1] = {'', '', '', empty_msg, '', ''}
		else
			overall_res[1] = {'', '', '', empty_msg, ''}
		end
	end

	-- build up output table: get length for each column to avoid exceptions when displaying output
	local length_schema      = getMaxLengthForColumn(overall_res, 1)
	local length_table       = getMaxLengthForColumn(overall_res, 2)
	local length_column      = getMaxLengthForColumn(overall_res, 3)
	local length_conversions = getMaxLengthForColumn(overall_res, 4)
	local length_query       = getMaxLengthForColumn(overall_res, 5)

	if apply_conversion then
		local length_success = getMaxLengthForColumn(overall_res, 6)
		exit(overall_res, "schema_name char("..length_schema.."), table_name char("..length_table.."), column_name char("..length_column.."), conversion char("..length_conversions.."), query_text char("..length_query.."), success char("..length_success..")")
	else
		exit(overall_res, "schema_name char("..length_schema.."), table_name char("..length_table.."), column_name char("..length_column.."), conversion char("..length_conversions.."), query_text char("..length_query..")")
	end
/


-- ====================================================================================
-- !!! ATTENTION: run with apply_conversion = false first and REVIEW the output.   !!!
-- !!! Only set apply_conversion = true when you are 100% sure, or - safer - copy   !!!
-- !!! the generated statements and execute them yourself, one by one.              !!!
-- ====================================================================================
-- If executed with 'false' --> Script only displays what changes would be made
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
'MY_SCHEMA',  --  schema_name:       SCHEMA name or SCHEMA_FILTER (can be %)
'%',          --  table_name:        TABLE name or TABLE_FILTER  (can be %)
true,         --  convert_double:    DOUBLE       -> smallest fitting DECIMAL(p,0) / DECIMAL(p,s)
true,         --  convert_integer:   DECIMAL(p,0) -> DECIMAL(9,0) / DECIMAL(18,0)
true,         --  convert_decimal:   DECIMAL(p,s) -> DECIMAL(9,s) / DECIMAL(18,s)
true,         --  convert_timestamp: TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE -> DATE
true,         --  convert_varchar:   VARCHAR(n)   -> smaller VARCHAR (same charset)
false,        --  log_for_all_columns: false = only report columns that change, true = report every inspected column
false         --  apply_conversion:  false = only report (recommended), true = irreversibly apply
);
