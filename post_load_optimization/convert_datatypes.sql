create schema if not exists database_migration;
/* 
	This script creates datatype optimizations for you. You can run this after
    importing your data. Selecting smaller datatypes might improve performance.

	This script:
  - looks at all columns of type 'DOUBLE' and converts them to numbers
	if only integer values are contained in the columns.
  - looks at all columns of type 'DECIMAL' and converts them to a smaller type
	of decimal if a smaller datatype is also sufficient.
  - looks at all columns of type 'TIMESTAMP' and converts them to date
	if only date values are contained in the columns.
  - looks at all columns of type 'VARCHAR' and converts them to a smaller type
    of varchar if a smaller VARCHAR can still hold the information in the column
*/

--parameter	schema_name: 	  SCHEMA name or SCHEMA_FILTER (can be %)
--parameter	table_name: 	  TABLE name or TABLE_FILTER  (can be %)	
--parameter	apply_conversion: Can be true or false, if true, columns are automatically converted to matching datatype. If false, only output of what would be changed is generated
--/
create or replace script database_migration.convert_datatypes(schema_name, table_name, apply_conversion) RETURNS TABLE
 as

function convert_double_to_decimal(schema_name, table_name, apply_conversion, log_for_all_columns)

	local result_table = {}
	res = query([[
			select
				column_schema,
				column_table,
				column_name
			from
				exa_all_columns
			where
				column_type_id = 8 and --type_id of DOUBLE
				column_schema like :schema_filter and -- e.g. '%' to convert all schema
				COLUMN_TABLE like :table_filter	--e.g. '%' to convert all tables
	]],{schema_filter=schema_name, table_filter=table_name})
	
	for i=1,#res do
			local message_suc = ''
			local message_action = ''
			--select overall rowcount (not null cols and rowcount with scale information (e.g. .0000000001)
			tsSuc, tsColumns = pquery([[select
					count(::col_name) "count", 'values_in_column' "title", 1 "order_column"
				from
					::curr_schema.::curr_table				
			union all
				select
					count(*) , 'cols_with_plain_decimals' , 2
				from
					::curr_schema.::curr_table
				where --select only columns with plain decimal (no scale)
					(
						not(
							abs(::col_name - cast(::col_name as decimal)) < 0.00000000001
								and
							 (
	--							::col_name is null
	--								or
								to_char(::col_name) ='0'
									or
								abs(::col_name) >= 1
							)
								and
							abs(::col_name) < 1E14
						)
					) order by 3 asc]]
				, {curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]),col_name=quote(res[i][3])});

			-- tsColumns[1][1] is the number of not_null values in this double column
			-- tsColumns[2][1] is the number of rows with plain decimals in this double column

			if not tsSuc then
				-- Datatype doesn't fit into decimal --> do nothing
				message_suc = 'Keep'
				message_action = 'Keep DOUBLE (too big for DECIMAL)'

			elseif tsColumns[1][1]==0 then
				--no rows in table -> do nothing
				message_suc = 'Keep'
				message_action = 'Keep DOUBLE (IS EMPTY)'

			elseif tsColumns[2][1]==0 then
				--rows in table but content seems to be decimal only (without scale information)
				if (apply_conversion) then
					local suc, res_query = pquery([[alter table ::curr_schema.::curr_table MODIFY (::col_name DECIMAL)]],
									{curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]), col_name=quote(res[i][3])})
					if suc then
						message_suc = 'true'
						message_action = 'DOUBLE --> DECIMAL'
					else
						message_suc = 'false'
						message_action = 'DOUBLE --> DECIMAL failed, ERROR: ' .. res_query.error_message .. ' Query was: ' .. res_query.statement_text
					end
					
				else -- case conversion is not applied
					message_suc = 'Not yet applied'
					message_action = 'DOUBLE --> DECIMAL'
				end

			else --real double
				message_suc = 'Keep'
				message_action = 'Keep DOUBLE'
				
			end
		

		if message_suc ~= 'Keep' or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
		end

	end
	return result_table
	end -- end of function convert_double_to_decimal
------------------------------------------------------------------------------------------------------
function convert_decimal_to_smaller_decimal(schema_name, table_name, apply_conversion, log_for_all_columns)

	local result_table = {}
	res = query([[select
				column_schema,
				column_table,
				column_name,
				COLUMN_NUM_PREC,COLUMN_NUM_SCALE
			from
				exa_all_columns
			where
				column_type_id = 3 and --type_id of DECIMAL
				column_schema like :schema_filter and -- e.g. '%' to convert all schema
				COLUMN_TABLE like :table_filter	--e.g. '%' to convert all tables
				and column_OBJECT_TYPE='TABLE'
				and COLUMN_NUM_SCALE = 0 -- for instance only handle values without scale
				and COLUMN_NUM_PREC >9
	]],{schema_filter=schema_name, table_filter=table_name})
	for i=1,#res do
			local message_suc = ''
			local message_action = ''

			scm = quote(res[i][1])
			tbl = quote(res[i][2])
			col = quote(res[i][3])
			dColumns = query([[select
					count(::col_name) "count", 'values_in_column' "title", 1 "order_column"
				from
					::curr_schema.::curr_table				
			union all
				select
					max(length(abs(::col_name))) , 'max_length' , 2
				from
					::curr_schema.::curr_table
			 order by 3 asc]], {curr_schema=scm, curr_table=tbl,col_name=col});
			
			-- dColumns[1][1] is the number of not_null values in this decimal column
			-- dColumns[2][1] is the max length in this decimal column
			if dColumns[1][1]==0 then

				--no rows in table -> do nothing
				message_suc = 'Keep'
				message_action = 'Keep DECIMAL (IS EMPTY)'

			elseif (dColumns[2][1]<=9 and res[i][4] > 9) or (dColumns[2][1]<=18 and res[i][4] > 18) then
				-- can find smaller datatype, either 32Bit or 64Bit
				change_from = res[i][4];
				if (dColumns[2][1]<=9 and res[i][4] > 9) then
				--fits into 32Bit
				change_to = 9;
				else
				--fits into 64Bit
				change_to=18;
				end
				if (apply_conversion) then
					local suc, res_query = pquery([[alter table ::curr_schema.::curr_table MODIFY (::col_name DECIMAL(:new_type))]],
									{curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]), col_name=quote(res[i][3]), new_type=change_to})
					if suc then
						message_suc = 'true'
						message_action = 'DECIMAL('..change_from..')  --> DECIMAL('..change_to..'), max length: '..dColumns[2][1]
					else
						message_suc = 'false'
						message_action = 'DECIMAL('..change_from..')  --> DECIMAL('..change_to..') failed, ERROR: ' .. res_query.error_message .. ' Query was: ' .. res_query.statement_text
					end
				else -- conversion not applied
					message_suc = 'Not yet applied'
					message_action = 'DECIMAL('..change_from..')  --> DECIMAL('..change_to..')'
				end
			else
				message_suc = 'Keep'
				message_action = 'Keep DECIMAL('..res[i][4]..'), max length: '..dColumns[2][1]
			end

		if message_suc ~= 'Keep' or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
		end
		end
	return result_table
	end -- end of function convert_decimal_to_smaller_decimal
	
------------------------------------------------------------------------------------------------------
function convert_timestamp_to_date(schema_name, table_name, apply_conversion, log_for_all_columns)
result_table = {};
res = query([[
		select
			column_schema,
			column_table,
			column_name
		from
			exa_all_columns
		where
			column_type_id = 93 and --type_id of TIMESTAMP
			column_schema like :schema_filter and -- e.g. '%' to convert all schema
			COLUMN_TABLE like :table_filter	--e.g. '%' to convert all tables
]],{schema_filter=schema_name, table_filter=table_name})

for i=1,#res do
		local message_suc = ''
		local message_action = ''

		--select overall rowcount (not null cols and rowcount with time information (e.g. one millisecond)
		tsColumns = query([[select
				count(::col_name) "count", 'values_in_column' "title", 1 "order_column"
			from
				::curr_schema.::curr_table				
			where
				::col_name is not null
		union all
			select
				count(*) , 'cols_with_plain_dates' , 2
			from
				::curr_schema.::curr_table
			where --select only columns with plain dates (no hours, minutes, seconds, fraction)
				(
						::col_name - to_date(::col_name) != INTERVAL '00:00:00.000000' HOUR
					TO
						SECOND
				) order by 3 asc]]
			, {curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]),col_name=quote(res[i][3])});


		-- tsColumns[1][1] is the number of not_null values in this timestamp column
		-- tsColumns[2][1] is the number of rows with plain dataes

		if tsColumns[1][1]==0 then
			--no rows in table -> do nothing
			message_suc = 'Keep'
			message_action = 'Keep TIMESTAMP (IS EMPTY)'

		elseif tsColumns[2][1]==0 then
			--rows in table but seems to be date only (without time information)
			if (apply_conversion) then
				local suc, res_query = pquery([[alter table ::curr_schema.::curr_table MODIFY (::col_name DATE)]],
									{curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]), col_name=quote(res[i][3])})
				if suc then
						message_suc = 'true'
						message_action = 'TIMESTAMP --> DATE'
				else
						message_suc = 'false'
						message_action = 'TIMESTAMP --> DATE failed, ERROR: ' .. res_query.error_message .. ' Query was: ' .. res_query.statement_text
				end
			else -- no conversion applied
				message_suc = 'Not yet applied'
				message_action = 'TIMESTAMP --> DATE'
			end
		else
			--real timestamp
			message_suc = 'Keep'
			message_action = 'Keep TIMESTAMP (IS EMPTY)'
		end
	if message_suc ~= 'Keep' or log_for_all_columns then
		result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
	end
	end
	return result_table
end -- end of function convert_timestamp_to_date

------------------------------------------------------------------------------------------------------
function convert_varchar_to_smaller_varchar(schema_name, table_name, apply_conversion, log_for_all_columns)

	local result_table = {}
	res = query([[select
				column_schema,
				column_table,
				column_name,
				column_maxsize
			from
				exa_all_columns
			where
				column_type_id = 12 and --type_id of VARCHAR
				column_schema like :schema_filter and -- e.g. '%' to convert all schema
				COLUMN_TABLE like :table_filter	--e.g. '%' to convert all tables
				and column_OBJECT_TYPE='TABLE'
	]],{schema_filter=schema_name, table_filter=table_name})
	for i=1,#res do
			local message_suc = ''
			local message_action = ''

			scm 			= quote(res[i][1])
			tbl				= quote(res[i][2])
			col 			= quote(res[i][3])
			current_maxsize = res[i][4]


			dColumns = query([[select
					count(::col_name) "count", 'values_in_column' "title", 1 "order_column"
				from
					::curr_schema.::curr_table				
			union all
				select
					max(length(::col_name)), 'actual maxlength', 2
				from
					::curr_schema.::curr_table
			 order by 3 asc]], {curr_schema=scm, curr_table=tbl,col_name=col});
			
	
			if dColumns[1][1]==0 then

				--no rows in table -> do nothing
				message_suc = 'Keep'
				message_action = 'Keep VARCHAR('..current_maxsize..') (IS EMPTY)'

			elseif (dColumns[2][1]<= 2000000) and (estimate_optimal_varchar_length(dColumns[2][1]) < current_maxsize) then
				-- can find smaller varchar and still smaller than 2 mio
				change_from = current_maxsize
				change_to = estimate_optimal_varchar_length(dColumns[2][1]) -- actual varchar size + a bit of buffer
				if (apply_conversion) then
					local suc, res_query = pquery([[alter table ::curr_schema.::curr_table MODIFY (::col_name VARCHAR(:new_type));]],
									{curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]), col_name=quote(res[i][3]), new_type=change_to})
					if suc then
						message_suc = 'true'
						message_action = 'VARCHAR('..change_from..')  --> VARCHAR('..change_to..'), max length: '..dColumns[2][1]
					else
						message_suc = 'false'
						message_action = 'VARCHAR('..change_from..')  --> VARCHAR('..change_to..') failed, ERROR: ' .. res_query.error_message .. ' Query was: ' .. res_query.statement_text
					end
				else -- conversion not applied
					message_suc = 'Not yet applied'
					message_action = 'VARCHAR('..change_from..')  --> VARCHAR('..change_to..'), max length: '..dColumns[2][1]
				end
			else
				message_suc = 'Keep'
				message_action = 'Keep VARCHAR('..res[i][4]..'), max length: '..dColumns[2][1]
			end

		if message_suc ~= 'Keep' or log_for_all_columns then
			result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
		end
		end
	return result_table
	end -- end of function convert_varchar_to_smaller_varchar


------------------------------------------------------------------------------------------------------

-- Helper function for varchar conversion, takes number as input, adds 20% and rounds to next bigger round decimal
function estimate_optimal_varchar_length(n)
	n_p20 = math.ceil(n + n*(0.2)) -- add 20% to make sure everything fits in
	number_digits = string.len(n_p20)
	magnitude = math.floor(10^(number_digits-1))
	estimated_length = math.floor(n_p20/magnitude)*magnitude+magnitude
	return math.min(estimated_length, 2000000) -- maximal varchar size is 2 mio
end

-- Helper function to get all actions into one result set
function merge_tables(t1, t2)
	for i = 1, #t2 do 
    	t1[#t1+1] = t2[i]
	end
	return t1
end

-- Helper function to get maximal string length for a column
function getMaxLengthForColumn(input_table, column_number)
	length = 1
	for i=1, #input_table do
		if(#input_table[i][column_number] > length) then
			length = #input_table[i][column_number]
		end
	end
	return length
end
-----------------------------END OF FUNCTIONS, BEGINNING OF ACTUAL SCRIPT-----------------------------
	
	-- parameter to regulate amount of output
	-- if false, a log message is only generated for columns that will be changed, if true, a message will be displayed for every column
	log_for_all_columns = false

	local overall_res = {}
	local res_double 	= convert_double_to_decimal(schema_name, table_name, apply_conversion, log_for_all_columns)
	local res_dec 		= convert_decimal_to_smaller_decimal(schema_name, table_name, apply_conversion, log_for_all_columns)
	local res_timestamp = convert_timestamp_to_date(schema_name, table_name, apply_conversion, log_for_all_columns)
	local res_varchar	= convert_varchar_to_smaller_varchar(schema_name, table_name, apply_conversion, log_for_all_columns)
	
	
	overall_res = merge_tables(overall_res, res_double)
	overall_res = merge_tables(overall_res, res_dec)
	overall_res = merge_tables(overall_res, res_timestamp)
	overall_res = merge_tables(overall_res, res_varchar)


	-- build up output table: get length for each column to avoid exceptions when displaying output
	length_schema 	= getMaxLengthForColumn(overall_res,1)
	length_table 	= getMaxLengthForColumn(overall_res,2)
	length_column 	= getMaxLengthForColumn(overall_res,3)
	length_success 	= getMaxLengthForColumn(overall_res,4)
	length_actions 	= getMaxLengthForColumn(overall_res,5)

	exit(overall_res, "schema_name char("..length_schema.."), table_name char("..length_table.."), column_name char("..length_column.."),success char("..length_success.."), actions char("..length_actions..")")
/


-- If executed with 'false' --> Script only displays what changes would be made
execute script DATABASE_MIGRATION.CONVERT_DATATYPES(
'MY_SCHEMA',		--	schema_name: 	  SCHEMA name or SCHEMA_FILTER (can be %)
'%', 				-- 	table_name: 	  TABLE name or TABLE_FILTER  (can be %)	
false				--	apply_conversion: If false, only output of what would be changed is generated, if true conversions are applied
);

