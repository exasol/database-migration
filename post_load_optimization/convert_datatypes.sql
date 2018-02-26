create schema if not exists database_migration;
/* 
	This script creates datatype optimizations for you. You can run this after
    importing your data. Selecting smaller datatypes might improve performance.

	This script:
  - looks at all columns of type 'DOUBLE' and converts them to numbers
	if only integer values are contained in the columns.
  - looks at all columns of type 'DECIMAL' and converts them to a smaller type
	of decimal if only smaller datatype is also sufficient.
  - looks at all columns of type 'TIMESTAMP' and converts them to date
	if only date values are contained in the columns.
*/

--parameter	schema_name: 	  SCHEMA name or SCHEMA_FILTER (can be %)
--parameter	table_name: 	  TABLE name or TABLE_FILTER  (can be %)	
--parameter	apply_conversion: Can be true or false, if true, columns are automatically converted to matching datatype. If false, only output of what would be changed is generated
create or replace script database_migration.convert_datatypes(schema_name, table_name, apply_conversion) RETURNS TABLE
 as
function convert_double_to_decimal(schema_name, table_name, apply_conversion)

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
			tsColumns = query([[select
					count(*),1
				from
					::curr_schema.::curr_table				
				where
					::col_name is not null
			union all
				select
					count(*),2
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
					) order by 2 asc]]
				, {curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]),col_name=quote(res[i][3])});
			if tsColumns[1][1]==0 then
				--no rows in table -> do nothing
				message_suc = 'Keep'
				message_action = 'Keep DECIMAL (IS EMPTY)'

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
		result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
		end
	return result_table
	end -- end of function convert_double_to_decimal
------------------------------------------------------------------------------------------------------
function convert_decimal_to_smaller_decimal(schema_name, table_name, apply_conversion)

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
					count(::col_name),1
				from
					::curr_schema.::curr_table				
			union all
				select
					max(length(abs(::col_name))),2
				from
					::curr_schema.::curr_table
			 order by 2 asc]], {curr_schema=scm, curr_table=tbl,col_name=col});
			
	
			if dColumns[1][1]==0 then
				--no rows in table -> do nothing
				message_suc = 'keep'
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

		result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
		end
	return result_table
	end -- end of function convert_decimal_to_smaller_decimal
	
------------------------------------------------------------------------------------------------------
function convert_timestamp_to_date(schema_name, table_name, apply_conversion)
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
				count(*),1
			from
				::curr_schema.::curr_table				
			where
				::col_name is not null
		union all
			select
				count(*),2
			from
				::curr_schema.::curr_table
			where --select only columns with plain dates (no hours, minutes, seconds, fraction)
				(
						::col_name - to_date(::col_name) != INTERVAL '00:00:00.000000' HOUR
					TO
						SECOND
				) order by 2 asc]]
			, {curr_schema=quote(res[i][1]), curr_table=quote(res[i][2]),col_name=quote(res[i][3])});
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
	result_table[#result_table+1] = {res[i][1], res[i][2], res[i][3],message_suc, message_action}
	end
	return result_table
end -- end of function convert_timestamp_to_date
------------------------------------------------------------------------------------------------------
-- Helper function to get all actions into one result set
function merge_tables(t1, t2)
	for i = 1, #t2 do 
    	t1[#t1+1] = t2[i]
	end
	return t1
end

-- Helper function to get maximal string length for a column
function getMaxLengthForColumn(input_table, column_number)
	length = 0
	for i=1, #input_table do
		if(#input_table[i][column_number] > length) then
			length = #input_table[i][column_number]
		end
	end
	return length
end
-----------------------------END OF FUNCTIONS, BEGINNING OF ACTUAL SCRIPT-----------------------------
	
	local overall_res = {}
	local res_double 	= convert_double_to_decimal(schema_name, table_name, apply_conversion)
	local res_dec 		= convert_decimal_to_smaller_decimal(schema_name, table_name, apply_conversion)
	local res_timestamp = convert_timestamp_to_date(schema_name, table_name, apply_conversion)
	
	
	overall_res = merge_tables(overall_res, res_double)
	overall_res = merge_tables(overall_res, res_dec)
	overall_res = merge_tables(overall_res, res_timestamp)


	-- build up output table: get length for each column to avoid exceptions when displaying output
	length_schema 	= getMaxLengthForColumn(overall_res,1)
	length_table 	= getMaxLengthForColumn(overall_res,2)
	length_column 	= getMaxLengthForColumn(overall_res,3)
	length_success 	= getMaxLengthForColumn(overall_res,4)
	length_actions 	= getMaxLengthForColumn(overall_res,5)

	exit(overall_res, "schema_name char("..length_schema.."), table_name char("..length_table.."), column_name char("..length_column.."),success char("..length_success.."), actions char("..length_actions..")")
/

------------------------------------------------------------------------------------------------------
---------------------- EXAMPLE FOR USAGE OF SCRIPT ---------------------------------------------------
------------------------------------------------------------------------------------------------------
create schema if not exists DATATYPES;

CREATE OR REPLACE TABLE DATATYPES.DATATYPE_TEST (
    "DOUBLE" DOUBLE,
    DOUBLE_TO_CONVERT DOUBLE,
	DOUBLE_TO_CONVERT2 DOUBLE,
	DOUBLE_NULL DOUBLE,
    TIMESTAMP_REAL TIMESTAMP,
	TIMESTAMP_TO_CONVERT TIMESTAMP,
	TIMESTAMP_TO_KEEP_WITH_NULL TIMESTAMP,
	"TIMESTAMP" TIMESTAMP,
	"WEIRDCOL'NAME1" DOUBLE,
	"WEIRDCOL'NAME2" DECIMAL(9),
	"WEIRDCOL'NAME3" TIMESTAMP
);

INSERT INTO DATATYPES.DATATYPE_TEST VALUES 
(1.2,1,1.000000000001,null,'1000-01-01 00:00:00.000', '1000-01-01 00:00:00.000', '1999-01-01 00:00:00.001', '1000-01-01 00:00:01.000',1,2, '1000-01-01 00:00:00.000')
,(2.1,2,2.000000000001,null,'2012-01-01 01:23:31.000', '1999-01-01 00:00:00.000', null, '1999-01-01 23:59:59.999',1,2, '1000-01-01 00:00:00.000' )
;

-- If executed with 'false' --> Script only displays what changes would be made 
execute script DATABASE_MIGRATION.convert_datatypes('DATATYPES','DATATYPE%', false);

-- If executed with 'true' --> Script applies changes
execute script DATABASE_MIGRATION.convert_datatypes('DATATYPES','DATATYPE%', true);

