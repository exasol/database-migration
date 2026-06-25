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

    The "conversion" column always names the EXACT current data type on the left and the EXACT target type
    on the right (e.g. DECIMAL(20, 0) --> DECIMAL(9, 0), TIMESTAMP(6) WITH LOCAL TIME ZONE --> DATE,
    VARCHAR(200) ASCII --> VARCHAR(20) ASCII). With log_for_all_columns = TRUE every inspected column of an
    enabled type is listed, including a "Keep ..." row for columns that are already minimal (DECIMAL
    precision <= 9, VARCHAR size <= 3) or empty.

    FOREIGN KEY handling (automatic): in Exasol a type change on a PRIMARY/FOREIGN KEY column fails unless
    the linked PK and FK columns are changed to the SAME type. So when FOREIGN KEYs touch the analyzed
    tables, the script (a) harmonizes every referential key group to ONE common target type that fits all of
    its columns, and (b) wraps the output / execution in a "DROP FOREIGN KEYS" step (first) and a
    "RE-ADD FOREIGN KEYS" step (last) - each FK is re-added in its ORIGINAL ENABLE/DISABLE state. A key
    column whose group reaches a table OUTSIDE the current filter is kept unchanged, with a note. With no
    FOREIGN KEYs in scope the run is unchanged (one cheap catalog check is the only overhead).

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
         one statement at a time, checking each result. Run the statements IN THE ORDER
         shown (DROP FOREIGN KEYS first, the MODIFYs, then RE-ADD FOREIGN KEYS).
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

-- Takes a number as input, adds 20% headroom and rounds up to the next bigger round decimal.
function estimate_optimal_varchar_length(n)
	local n_p20            = math.ceil(n + n * 0.2)               -- add 20% so everything fits in
	local number_digits    = string.len(n_p20)
	local magnitude        = math.floor(10 ^ (number_digits - 1))
	local estimated_length = math.floor(n_p20 / magnitude) * magnitude + magnitude
	return math.min(estimated_length, 2000000)                   -- maximal varchar size is 2,000,000
end

------------------------------------------------------------------------------------------------------
-- Renders the EXACT target type string from a harmonized group target (or a per-column target).
------------------------------------------------------------------------------------------------------
function render_target(t)
	if     t.fam == 'DEC0' then return "DECIMAL("..t.target..", 0)"
	elseif t.fam == 'DECS' then return "DECIMAL("..t.target..", "..t.s..")"
	elseif t.fam == 'DBL'  then return "DECIMAL("..t.p..", "..t.s..")"
	elseif t.fam == 'TS'   then return "DATE"
	elseif t.fam == 'VC'   then return "VARCHAR("..t.target..") "..t.cs
	else                        return nil end -- KEEP
end

------------------------------------------------------------------------------------------------------
-- Decides ONE common target for a referential key group (all members share the same source type,
-- because an FK requires identical types). Returns a target descriptor or {fam='KEEP'} when the
-- group cannot be converted (then the columns stay as they are and the FK remains valid).
------------------------------------------------------------------------------------------------------
function group_target(members)
	for i = 1, #members do
		if members[i].tgt.fam == 'KEEP' then return {fam='KEEP'} end
	end
	local fam = members[1].tgt.fam
	if fam == 'DEC0' then
		local need = 0
		for i = 1, #members do if members[i].tgt.need > need then need = members[i].tgt.need end end
		local target = smaller_integer_precision(need, members[1].tgt.p)
		if target == nil then return {fam='KEEP'} end
		return {fam='DEC0', target=target}
	elseif fam == 'DECS' then
		local need = 0
		for i = 1, #members do if members[i].tgt.need > need then need = members[i].tgt.need end end
		local target = smaller_integer_precision(need, members[1].tgt.p)
		if target == nil then return {fam='KEEP'} end
		return {fam='DECS', target=target, s=members[1].tgt.s}
	elseif fam == 'VC' then
		local ml = 0
		for i = 1, #members do if members[i].tgt.maxlen > ml then ml = members[i].tgt.maxlen end end
		local tl = estimate_optimal_varchar_length(ml)
		if tl < members[1].tgt.n then return {fam='VC', target=tl, cs=members[1].tgt.cs} end
		return {fam='KEEP'}
	elseif fam == 'TS' then
		for i = 1, #members do if members[i].tgt.has_time then return {fam='KEEP'} end end
		return {fam='TS'}
	elseif fam == 'DBL' then
		local p, s = members[1].tgt.p, members[1].tgt.s
		for i = 1, #members do
			if not members[i].tgt.conv or members[i].tgt.p ~= p or members[i].tgt.s ~= s then return {fam='KEEP'} end
		end
		return {fam='DBL', p=p, s=s}
	end
	return {fam='KEEP'}
end

------------------------------------------------------------------------------------------------------
-- DOUBLE --> DECIMAL
------------------------------------------------------------------------------------------------------
function convert_double_to_decimal(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name
			from   exa_all_columns
			where  column_type_id = 8 and             -- type_id of DOUBLE
			       column_schema like :schema_filter and
			       column_table  like :table_filter and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local src = 'DOUBLE'

		local tsSuc, tsColumns = pquery([[
			select
				count(::col_name) as values_in_column,
				count(case when
					not(
						abs(::col_name - cast(::col_name as decimal)) < 0.00000000001
							and
						( to_char(::col_name) = '0' or abs(::col_name) >= 1 )
							and
						abs(::col_name) < 1E14
					)
				then 1 end) as non_integer_rows,
				coalesce(max(length(cast(abs(::col_name) as decimal(18,0)))), 0) as max_int_length
			from ::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		if not tsSuc then
			message_action = 'Keep DOUBLE (values exceed DECIMAL range)'
		elseif tsColumns[1][1] == 0 then
			message_action = 'Keep DOUBLE (empty)'
		elseif tsColumns[1][2] == 0 then
			local max_int_length = tsColumns[1][3]
			local target = 9
			if max_int_length > 9 then target = 18 end
			query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..",0));"
			modify_column    = true
			message_action   = src..' --> DECIMAL('..target..', 0), max length: '..max_int_length
			tgt = {fam='DBL', conv=true, p=target, s=0}
		else
			local s, max_int_digits = detect_lossless_double_scale(scm, tbl, col)
			if s ~= nil then
				local p = max_int_digits + s
				local target = 9
				if p > 9 then target = 18 end
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..","..s.."));"
				modify_column    = true
				message_action   = src..' --> DECIMAL('..target..', '..s..') (lossless)'
				tgt = {fam='DBL', conv=true, p=target, s=s}
			else
				message_action = 'Keep DOUBLE (not losslessly representable as DECIMAL)'
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- DECIMAL(p,0) --> smaller integer DECIMAL
------------------------------------------------------------------------------------------------------
function convert_integer_to_smaller_integer(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_num_prec
			from   exa_all_columns
			where  column_type_id = 3 and             -- type_id of DECIMAL
			       column_schema like :schema_filter and
			       column_table  like :table_filter and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE and
			       column_num_scale = 0                -- only columns without scale
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local precision = res[i][4]
		local src = 'DECIMAL('..precision..', 0)'

		if precision <= 9 then
			-- already the smallest integer type -> no scan needed
			message_action = 'Keep '..src..' (already minimal precision)'
		else
			local dColumns = query([[
				select count(::col_name) as values_in_column,
				       coalesce(max(length(abs(::col_name))), 0) as max_length
				from ::curr_schema.::curr_table
			]], {curr_schema=scm, curr_table=tbl, col_name=col})
			local not_null_count, max_length = dColumns[1][1], dColumns[1][2]
			if not_null_count == 0 then
				message_action = 'Keep '..src..' (empty)'
				tgt = {fam='DEC0', p=precision, need=0}   -- empty: no constraint for a key group
			else
				local target = smaller_integer_precision(max_length, precision)
				tgt = {fam='DEC0', p=precision, need=max_length}
				if target ~= nil then
					query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..",0));"
					modify_column    = true
					message_action   = src..' --> DECIMAL('..target..', 0), max length: '..max_length
				else
					message_action = 'Keep '..src..', max length: '..max_length
				end
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- DECIMAL(p,s) --> smaller DECIMAL (scale preserved)
------------------------------------------------------------------------------------------------------
function convert_decimal_with_scale_to_smaller_decimal(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_num_prec, column_num_scale
			from   exa_all_columns
			where  column_type_id = 3 and             -- type_id of DECIMAL
			       column_schema like :schema_filter and
			       column_table  like :table_filter and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE and
			       column_num_scale <> 0               -- only columns that have a scale
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local precision, col_scale = res[i][4], res[i][5]
		local src = 'DECIMAL('..precision..', '..col_scale..')'

		if precision <= 9 then
			message_action = 'Keep '..src..' (already minimal precision)'
		else
			local dColumns = query([[
				select count(::col_name) as values_in_column,
				       coalesce(max(length(floor(abs(::col_name)))), 0) as max_int_length
				from ::curr_schema.::curr_table
			]], {curr_schema=scm, curr_table=tbl, col_name=col})
			local not_null_count, max_int_length = dColumns[1][1], dColumns[1][2]
			local needed_precision = max_int_length + col_scale
			if not_null_count == 0 then
				message_action = 'Keep '..src..' (empty)'
				tgt = {fam='DECS', p=precision, s=col_scale, need=col_scale}
			else
				local target = smaller_integer_precision(needed_precision, precision)
				tgt = {fam='DECS', p=precision, s=col_scale, need=needed_precision}
				if target ~= nil then
					query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DECIMAL("..target..","..col_scale.."));"
					modify_column    = true
					message_action   = src..' --> DECIMAL('..target..', '..col_scale..'), needed precision: '..needed_precision
				else
					message_action = 'Keep '..src..', needed precision: '..needed_precision
				end
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE --> DATE
------------------------------------------------------------------------------------------------------
function convert_timestamp_to_date(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_type
			from   exa_all_columns
			where  column_type_id in (93, 124) and    -- 93 = TIMESTAMP, 124 = TIMESTAMP WITH LOCAL TIME ZONE
			       column_schema like :schema_filter and
			       column_table  like :table_filter and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local src = res[i][4]   -- exact current type from the catalog, e.g. "TIMESTAMP(6)" / "TIMESTAMP(3) WITH LOCAL TIME ZONE"

		local tsSuc, tsColumns = pquery([[
			select count(::col_name) as values_in_column,
			       count(case when ::col_name <> TRUNC(::col_name) then 1 end) as rows_with_time
			from ::curr_schema.::curr_table
		]], {curr_schema=scm, curr_table=tbl, col_name=col})

		if not tsSuc then
			message_action = 'Keep '..src..' (check failed)'
		elseif tsColumns[1][1] == 0 then
			message_action = 'Keep '..src..' (empty)'
			tgt = {fam='TS', has_time=false}   -- empty: convertible if the rest of its key group is
		elseif tsColumns[1][2] == 0 then
			query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." DATE);"
			modify_column    = true
			message_action   = src..' --> DATE'
			tgt = {fam='TS', has_time=false}
		else
			message_action = 'Keep '..src..' (has a time component)'
			tgt = {fam='TS', has_time=true}
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- VARCHAR(n) --> smaller VARCHAR (charset preserved)
------------------------------------------------------------------------------------------------------
function convert_varchar_to_smaller_varchar(schema_name, table_name)
	local result_table = {}
	local res = query([[
			select column_schema, column_table, column_name, column_maxsize, column_type
			from   exa_all_columns
			where  column_type_id = 12 and            -- type_id of VARCHAR
			       column_schema like :schema_filter and
			       column_table  like :table_filter and
			       column_object_type = 'TABLE' and
			       column_is_virtual  = FALSE
		]], {schema_filter=schema_name, table_filter=table_name})

	for i=1,#res do
		local modify_column, message_action, query_to_execute = false, '', ''
		local tgt = {fam='KEEP'}
		local scm, tbl, col = quote(res[i][1]), quote(res[i][2]), quote(res[i][3])
		local current_maxsize = res[i][4]
		local charset = 'UTF8'
		if string.find(string.upper(res[i][5]), 'ASCII', 1, true) ~= nil then charset = 'ASCII' end
		local src = 'VARCHAR('..current_maxsize..') '..charset

		if current_maxsize <= 3 then
			message_action = 'Keep '..src..' (n <= 3, left untouched)'
		else
			local dColumns = query([[
				select count(::col_name) as values_in_column,
				       coalesce(max(length(::col_name)), 0) as max_length
				from ::curr_schema.::curr_table
			]], {curr_schema=scm, curr_table=tbl, col_name=col})
			local not_null_count, max_length = dColumns[1][1], dColumns[1][2]
			if not_null_count == 0 then
				message_action = 'Keep '..src..' (empty)'
				tgt = {fam='VC', n=current_maxsize, cs=charset, maxlen=0}
			elseif (max_length <= 2000000) and (estimate_optimal_varchar_length(max_length) < current_maxsize) then
				local change_to  = estimate_optimal_varchar_length(max_length)
				query_to_execute = "ALTER TABLE "..scm.."."..tbl.." MODIFY ("..col.." VARCHAR("..change_to..") "..charset..");"
				modify_column    = true
				message_action   = src..' --> VARCHAR('..change_to..') '..charset..', max length: '..max_length
				tgt = {fam='VC', n=current_maxsize, cs=charset, maxlen=max_length}
			else
				message_action = 'Keep '..src..', max length: '..max_length
				tgt = {fam='VC', n=current_maxsize, cs=charset, maxlen=max_length}
			end
		end
		result_table[#result_table+1] = {sch=res[i][1], tab=res[i][2], col=res[i][3], src=src,
			message=message_action, query=query_to_execute, modify=modify_column, tgt=tgt}
	end
	return result_table
end

------------------------------------------------------------------------------------------------------
-- Helper functions
------------------------------------------------------------------------------------------------------

-- Appends all rows of t2 to t1 and returns t1.
function merge_tables(t1, t2)
	for i = 1, #t2 do t1[#t1+1] = t2[i] end
	return t1
end

-- Returns the maximal string length found in one column of the (positional) result table.
function getMaxLengthForColumn(input_table, column_number)
	local length = 1
	for i = 1, #input_table do
		local v = input_table[i][column_number]
		if v ~= nil and #v > length then length = #v end
	end
	return length
end

-- Executes the SQL in one column of the positional table and logs the outcome in another column.
-- Comment-only rows (section dividers starting with '--') and empty rows are skipped, NOT executed.
function execute_sql_column(input_table, sql_col_number, log_col_number)
	for i = 1, #input_table do
		local sql_stmt = input_table[i][sql_col_number]
		if sql_stmt ~= nil and sql_stmt ~= '' and string.sub(sql_stmt, 1, 2) ~= '--' then
			local sql_suc, sql_res = pquery(sql_stmt)
			if sql_suc then
				input_table[i][log_col_number] = 'true'
			else
				input_table[i][log_col_number] = 'ERROR: ' .. sql_res.error_message
			end
		else
			input_table[i][log_col_number] = ''   -- nothing to execute (a "Keep" row or a section divider)
		end
	end
	return input_table
end

-----------------------------END OF FUNCTIONS, BEGINNING OF ACTUAL SCRIPT-----------------------------

	-- 1) Collect every inspected column of the ENABLED types (records, not yet filtered by log_for_all_columns)
	local analyzed = {}
	if convert_double    then analyzed = merge_tables(analyzed, convert_double_to_decimal(schema_name, table_name)) end
	if convert_integer   then analyzed = merge_tables(analyzed, convert_integer_to_smaller_integer(schema_name, table_name)) end
	if convert_decimal   then analyzed = merge_tables(analyzed, convert_decimal_with_scale_to_smaller_decimal(schema_name, table_name)) end
	if convert_timestamp then analyzed = merge_tables(analyzed, convert_timestamp_to_date(schema_name, table_name)) end
	if convert_varchar   then analyzed = merge_tables(analyzed, convert_varchar_to_smaller_varchar(schema_name, table_name)) end

	-- 2) FOREIGN KEY handling (only when FKs touch the analyzed tables; otherwise no effect)
	local function nodek(s, t, c) return s .. string.char(1) .. t .. string.char(1) .. c end
	local acol = {}
	for i = 1, #analyzed do acol[nodek(analyzed[i].sch, analyzed[i].tab, analyzed[i].col)] = analyzed[i] end
	local tabset = {}
	for i = 1, #analyzed do tabset[analyzed[i].sch .. string.char(1) .. analyzed[i].tab] = true end

	local fk_suc, fk = pquery([[
		SELECT cc.constraint_schema, cc.constraint_table, cc.constraint_name, cc.ordinal_position,
		       cc.column_name, cc.referenced_schema, cc.referenced_table, cc.referenced_column,
		       c.constraint_enabled
		FROM   exa_all_constraint_columns cc
		JOIN   exa_all_constraints c
		       ON  c.constraint_schema = cc.constraint_schema
		       AND c.constraint_table  = cc.constraint_table
		       AND c.constraint_name   = cc.constraint_name
		WHERE  cc.constraint_type = 'FOREIGN KEY'
		  AND  (cc.constraint_schema LIKE :schp1 OR cc.referenced_schema LIKE :schp2)
		ORDER  BY cc.constraint_schema, cc.constraint_table, cc.constraint_name, cc.ordinal_position
	]], {schp1 = schema_name, schp2 = schema_name})

	local drop_rows, readd_rows = {}, {}

	if fk_suc and #fk > 0 then
		local parent, nodedisp = {}, {}
		local function find(x)
			if parent[x] == nil then parent[x] = x end
			while parent[x] ~= x do parent[x] = parent[parent[x]]; x = parent[x] end
			return x
		end
		local function union(a, b) local ra, rb = find(a), find(b); if ra ~= rb then parent[ra] = rb end end
		local fkdef, fkorder = {}, {}
		for i = 1, #fk do
			local cs, ct, cn = fk[i][1], fk[i][2], fk[i][3]
			local cc         = fk[i][5]
			local rs, rt, rc = fk[i][6], fk[i][7], fk[i][8]
			local cnode, rnode = nodek(cs, ct, cc), nodek(rs, rt, rc)
			nodedisp[cnode] = '"'..cs..'"."'..ct..'"'; nodedisp[rnode] = '"'..rs..'"."'..rt..'"'
			union(cnode, rnode)
			local key = nodek(cs, ct, cn)
			if fkdef[key] == nil then
				fkdef[key] = {csch=cs, ctab=ct, cname=cn, cols={}, rsch=rs, rtab=rt, rcols={}, enabled=fk[i][9]}
				fkorder[#fkorder+1] = key
			end
			fkdef[key].cols[#fkdef[key].cols+1]   = cc
			fkdef[key].rcols[#fkdef[key].rcols+1] = rc
		end

		local groups = {}
		for nkey in pairs(parent) do
			local root = find(nkey)
			if groups[root] == nil then groups[root] = {} end
			groups[root][#groups[root]+1] = nkey
		end

		local converting = {}
		for root, nodes in pairs(groups) do
			local members, missing = {}, {}
			for _, nkey in ipairs(nodes) do
				if acol[nkey] ~= nil then members[#members+1] = acol[nkey] else missing[#missing+1] = nkey end
			end
			if #members > 0 then
				if #missing > 0 then
					local seen, rel = {}, {}
					for _, nkey in ipairs(missing) do
						local d = nodedisp[nkey] or '(unknown table)'
						if not seen[d] then seen[d] = true; rel[#rel+1] = d end
					end
					local relstr = table.concat(rel, ', ')
					for _, a in ipairs(members) do
						a.message = 'Keep '..a.src..' (FK key column - related table out of scope)'
						a.query = ''; a.modify = false
					end
				else
					local gt = group_target(members)
					local ttype = render_target(gt)
					if ttype ~= nil then
						converting[root] = true
						for _, a in ipairs(members) do
							a.query   = "ALTER TABLE "..quote(a.sch).."."..quote(a.tab).." MODIFY ("..quote(a.col).." "..ttype..");"
							a.message = a.src..' --> '..ttype..'  [FK key group - harmonized]'
							a.modify  = true
						end
					else
						for _, a in ipairs(members) do
							a.message = 'Keep '..a.src..' (FK key group: no common smaller type - not changed)'
							a.query = ''; a.modify = false
						end
					end
				end
			end
		end

		local function q(x) return '"'..x..'"' end
		for _, key in ipairs(fkorder) do
			local d = fkdef[key]
			local touches = false
			for _, cc in ipairs(d.cols) do
				if converting[find(nodek(d.csch, d.ctab, cc))] then touches = true; break end
			end
			if touches then
				local en    = d.enabled
				local state = (en == false or en == 'FALSE' or en == 'false' or en == 0 or en == '0') and 'DISABLE' or 'ENABLE'
				local ccols, rcols = {}, {}
				for _, c in ipairs(d.cols)  do ccols[#ccols+1]  = q(c) end
				for _, c in ipairs(d.rcols) do rcols[#rcols+1] = q(c) end
				drop_rows[#drop_rows+1] = {sch=d.csch, tab=d.ctab, col='', message='drop foreign key '..d.cname,
					query='ALTER TABLE '..q(d.csch)..'.'..q(d.ctab)..' DROP CONSTRAINT '..q(d.cname)..';'}
				readd_rows[#readd_rows+1] = {sch=d.csch, tab=d.ctab, col='', message='re-add foreign key '..d.cname..' ('..state..')',
					query='ALTER TABLE '..q(d.csch)..'.'..q(d.ctab)..' ADD CONSTRAINT '..q(d.cname)..
					      ' FOREIGN KEY ('..table.concat(ccols, ', ')..') REFERENCES '..q(d.rsch)..'.'..q(d.rtab)..
					      ' ('..table.concat(rcols, ', ')..') '..state..';'}
			end
		end
	end

	-- 3) Build the column rows honoring log_for_all_columns, sorted by schema/table/column
	local col_recs = {}
	for i = 1, #analyzed do
		if analyzed[i].modify or log_for_all_columns then col_recs[#col_recs+1] = analyzed[i] end
	end
	table.sort(col_recs, function(a, b)
		if a.sch ~= b.sch then return a.sch < b.sch end
		if a.tab ~= b.tab then return a.tab < b.tab end
		return a.col < b.col
	end)

	-- 4) Assemble the final ordered list of records (DROP FKs first, MODIFYs, RE-ADD FKs last)
	local ordered = {}
	if #drop_rows > 0 then
		ordered[#ordered+1] = {sch='', tab='', col='', message='', query='-- ### DROP FOREIGN KEYS - run these FIRST (before the column changes) ###'}
		for _, x in ipairs(drop_rows)  do ordered[#ordered+1] = x end
		ordered[#ordered+1] = {sch='', tab='', col='', message='', query='-- ### COLUMN TYPE CHANGES ###'}
		for _, x in ipairs(col_recs)   do ordered[#ordered+1] = x end
		ordered[#ordered+1] = {sch='', tab='', col='', message='', query='-- ### RE-ADD FOREIGN KEYS - run these LAST (after the column changes) ###'}
		for _, x in ipairs(readd_rows) do ordered[#ordered+1] = x end
	else
		ordered = col_recs
	end

	-- 5) Turn records into positional rows (add a success slot when applying)
	local overall_res = {}
	for i = 1, #ordered do
		local r = ordered[i]
		if apply_conversion then
			overall_res[#overall_res+1] = {r.sch, r.tab, r.col, r.message, r.query, ''}
		else
			overall_res[#overall_res+1] = {r.sch, r.tab, r.col, r.message, r.query}
		end
	end

	-- 6) Apply (in the assembled order: DROP -> MODIFY -> RE-ADD); section dividers are skipped
	if apply_conversion then
		overall_res = execute_sql_column(overall_res, 5, 6)
	end

	-- 7) Friendly message when nothing matched
	if #overall_res == 0 then
		local empty_msg
		if log_for_all_columns then
			empty_msg = 'No matching columns found (check the filters and the convert_* switches).'
		else
			empty_msg = 'No columns found that need optimization.'
		end
		if apply_conversion then overall_res[1] = {'', '', '', empty_msg, '', ''}
		else                     overall_res[1] = {'', '', '', empty_msg, ''} end
	end

	-- 8) Size the output columns and return (order is intentional - do NOT re-sort)
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
-- !!! the generated statements and execute them yourself, in the shown order.      !!!
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
