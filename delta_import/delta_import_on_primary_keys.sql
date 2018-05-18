--the delta_import script is designed to do a delta-load with the import statement, in order to load only new columns.
--to merge updated rows into the destination table primary keys are used and are expected for the destination table (primary keys can be deactivated / only metadata)
--in case there is no update column or primary keys -> a full load of the table is done.

-- conn_type: connection type -> one of the connection types the import statement support -> e.g. JDBC (to use with a generic database), EXA, ORA
-- conn_db: 	which database is this script importing from --> 'MYSQL' or 'EXASOL'
-- conn_name: name of the connection -> that connection has to be created upfront and is used for the import statements
-- source_schema_name: name of the schema in the source system
-- target_schema_name: name of the target schema (contains the table you want to import into) -> can also include wildcards for a like-clause
-- table_name: name of the table -> can include wildcard -> e.g. % to load the whole schema
-- delta_detection_column: name of the column used for delta loading -> if that column exists new or modified rows have to have an increased value compared to the last load (max is used) -> e.g. use an increasing job number in the source system or a modified timestamp.
-- staging_schema: name of a temporary schema used for storing tables into before merging them into the original tables
create or replace script database_migration.delta_import(conn_type, conn_db, conn_name, source_schema_name, target_schema_name, table_name, delta_detection_column, staging_schema) returns table
 as
-----------------------------------------------------------------------------------------
function quoteForDb(db_name, string_to_quote)

-- remove quotes if already set
string_to_quote = string.gsub(string_to_quote, '"', '')
escape_char = '"'
	if db_name == 'MYSQL' then
		escape_char = '`'
	end
	return escape_char .. string_to_quote .. escape_char
end
-----------------------------------------------------------------------------------------
function debug(table)
	if table == nil or table[1] == nil or table[1][1] == nil then
		return
	end

	print('--------------------------------------------')
	for i = 1,#table do
		tmp = ''
		for j = 1,#table[i] do
			local new_val = table[i][j]
			if new_val == nil or new_val == null then
				new_val = '-'
			end

			tmp = tmp .. new_val ..'	| '
		end
		print(tmp)
	end
	print('--------------------------------------------')
end

-----------------------------------------------------------------------------------------
function create_log_tables(log_schema, job_log_table, job_details_table)

query([[create schema if not exists ::s ]], {s= log_schema})
query([[
create table if not exists ::s.::jl(
			run_id int identity,
			script_name varchar(100),
			status varchar(100),
			start_time timestamp default systimestamp,
			end_time timestamp
		)
]], {s= staging_schema, jl=job_log_table})

query([[
		create table if not exists ::s.::jd (
			detail_id int identity,
			run_id int,
			log_time timestamp,
			log_level varchar(10),
			log_message varchar(2000),
			rowcount int
		)
]], {s= log_schema, jd=job_details_table})
end

-----------------------------------------------------------------------------------------
-- helper function for script, not intended to be used as standalone
-- following parameters must be set for wrapper:
-- curr_schema	wrapper:set_param('curr_schema', 'MY_VALUE')
-- curr_table	wrapper:set_param('curr_table', 'MY_VALUE')
-- delta_col	wrapper:set_param('delta_col', 'MY_VALUE')
-- following variables must be present:
-- conn_db, delta_detection_column, target_schema, target_table
function getMaxValue(conn_db, delta_detection_column, target_schema, target_table)

	-- get type of delta_detection_column: if timestamp/date --> convert to predefined format
	suc, delta_col_type = wrapper:query([[
			select COLUMN_TYPE from EXA_ALL_COLUMNS 
			where '"'|| column_schema || '"' = :curr_schema
			and '"' || column_table || '"' = :curr_table
			and '"' || column_name || '"' = :delta_col
			]])

	delta_col_type = delta_col_type[1][1]
		
	if (delta_col_type == 'TIMESTAMP' or delta_col_type == 'DATE') then
		maxQuery = [[to_char(max(::col_name), 'YYYY-MM-DD HH24:MI:SS.FF6')]]
	else
		maxQuery = 'max(::col_name)'
	end
	-- TODO: Can we use the wrapper here? Don't want to throw an error if statement doesn't succeed
	success, maxDeltaVal = pquery([[select ]].. maxQuery ..[[ from ::curr_schema.::curr_table]], {col_name=delta_detection_column, curr_schema=target_schema, curr_table=target_table});

	if success and maxDeltaVal[1][1] ~= NULL then
		if (delta_col_type == 'TIMESTAMP' or delta_col_type == 'DATE') then
			if(conn_db == 'MYSQL') then
				maxDelta = [[STR_TO_DATE(']]..maxDeltaVal[1][1]..[[', '%Y-%m-%d %H:%i:%s.%f')]]
			else
				maxDelta = [[to_timestamp(']]..maxDeltaVal[1][1]..[[', 'YYYY-MM-DD HH24:MI:SS.FF6')]]
			end
		else
			maxDelta = maxDeltaVal[1][1]
		end
		return success, maxDelta
	else
		return success, NULL
	end
end


------------
-- queries all needed information for tables inside target_schema matching table_filter within one resultset
-- following parameters must be set for wrapper:
-- target_schema 	wrapper:set_param('curr_schema', 'MY_VALUE')
-- table_filter 	wrapper:set_param('curr_schema', 'MY_VALUE')
--
-- outputs a table consisting of:
-- table_schema, table_name, all_columns, key_columns, no_key_columns
function getTargetTableInformation()

	res_key_col_infos = wrapper:query_values([[with constr as (
		select constraint_schema, constraint_table,	COLUMN_NAME
		from EXA_ALL_CONSTRAINT_COLUMNS
		where constraint_schema LIKE :target_schema and
			CONSTRAINT_TABLE LIKE :table_filter and
			CONSTRAINT_TYPE = 'PRIMARY KEY'
		order by 1,	2),
	cols as (
		select column_schema, column_table, column_name, column_ordinal_position
		from EXA_ALL_COLUMNS),
	no_key_cols as (
		select cols.column_schema, cols.column_table, cols.column_name
		from cols minus
		select constr.constraint_schema, constr.constraint_table, constr.column_name
		from constr	),
	all_tables as (
		select *
		from EXA_ALL_TABLES
		WHERE TABLE_SCHEMA LIKE :target_schema and
			  TABLE_NAME LIKE :table_filter	),
	tables_with_collist as (
		select cols.column_schema schema_name, cols.column_table table_name, 
			group_concat(distinct 'a.' || '"' || constr.column_name || '"' || '=b.' || '"' || constr.column_name || '"' SEPARATOR ' and ') key_columns,
			group_concat(distinct '"' || cols.column_name || '"' order by	column_ordinal_position	) all_columns,
			group_concat(distinct 'a.' || '"' || no_key_cols.column_name || '"' || '=b.' || '"' || no_key_cols.column_name || '"') no_key_columns
		from constr
			join cols on
				constr.constraint_schema = cols.column_schema and
				constr.constraint_table = cols.column_table
			join no_key_cols on
				no_key_cols.column_schema = cols.column_schema and
				no_key_cols.column_table = cols.column_table
		group by 1,2)
select
	'"' || all_tables.table_schema || '"' table_schema,
	'"' || all_tables.table_name || '"' table_name,
	all_columns,
	key_columns,
	no_key_columns
from tables_with_collist
	right join	all_tables on
		all_tables.table_schema = tables_with_collist.schema_name and
		all_tables.table_name = tables_with_collist.table_name
order by 1,2 asc
]])
	return res_key_col_infos
end



--------------------------- actual script -----------------------------------------------
-----------------------------------------------------------------------------------------


-- create schema and tables for log data
job_log_table= 'JOB_LOG'
job_details_table = 'JOB_DETAILS'
create_log_tables(staging_schema,job_log_table,job_details_table)

-- import query wrapper and tell it logging table names
import('ETL.QUERY_WRAPPER','QW')
wrapper = QW.new( staging_schema..'.'..job_log_table, staging_schema..'.'..job_details_table, 'delta_import_on_primary_keys')

-- setup all parameters
wrapper:set_param('conn_type', conn_type)
wrapper:set_param('conn_name', conn_name)
wrapper:set_param('staging_schema',staging_schema)
wrapper:set_param('target_schema',target_schema_name)
wrapper:set_param('table_filter',table_name)

delta_detection_column = quoteForDb('EXASOL', delta_detection_column)
wrapper:set_param('delta_col', delta_detection_column)

-- check if script is used on local tables
if string.upper(conn_type) == 'LOCAL' then
	local_conn = true
	conn_db = 'EXASOL'
else
	local_conn = false
end




res_key_col_infos = getTargetTableInformation()
--debug(res_key_col_infos)


--iterate through the resultset
for target_schema, target_table, all_columns, key_columns, no_key_columns in res_key_col_infos do 

	-- setup parameters
	wrapper:set_param('curr_schema', target_schema)
	wrapper:set_param('curr_table', target_table)
	if local_conn then
		insert_keyword = 'insert '
		importFromSrc = [[ (select * from ]]..quoteForDb(conn_db,source_schema_name)..[[.]]..quoteForDb(conn_db,target_table).. [[ )]]
	else
		insert_keyword = 'import '
		selectStarStmt = [[select * from ]]..quoteForDb(conn_db,source_schema_name)..[[.]]..quoteForDb(conn_db,target_table)
		wrapper:set_param('selectStmt', selectStarStmt)
		importFromSrc = [[ from ::conn_type at ::conn_name statement :selectStmt]]
	end
	wrapper:set_param('insert_keyword', insert_keyword)



	if key_columns == NULL then
		--no key column -> do a full load, no need for using the staging table
		wrapper:query([[truncate table ::curr_schema.::curr_table]]);
		suc, currRowCount = wrapper:query(insert_keyword ..[[ into ::curr_schema.::curr_table]].. importFromSrc)
		local tmpRowCount = currRowCount.etl_rows_written
		wrapper:log('LOG',target_schema..'.'..target_table..' has no key column -> TRUNCATE and LOAD', tmpRowCount)

	else
		--there is a key column -> create a staging table and try to identify the maximum of the update column to load the delta
		wrapper:query('create or replace table ::staging_schema.::curr_table like ::curr_schema.::curr_table');


		maxDeltaColExists, maxDelta = getMaxValue(conn_db, delta_detection_column, target_schema, target_table)

		
		if not maxDeltaColExists then	--delta-detection-column doesn't exist in target or target table is already empty -> do a truncate and then a load
			wrapper:query([[truncate table ::curr_schema.::curr_table]]);
			suc, currRowCount = wrapper:query(insert_keyword ..[[ into ::curr_schema.::curr_table]]..importFromSrc)
			local tmpRowCount = currRowCount.etl_rows_written
			wrapper:log('LOG',target_schema..'.'..target_table..' has key column, but no column '..delta_detection_column..' -> TRUNCATE and LOAD', tmpRowCount)

		else --querying max of delta detection column was successful -> load into staging table

			if maxDelta == NULL then	-- no maximum in delta detection column found  -> do a full load into the staging table
				suc, currRowCount = wrapper:query(insert_keyword ..[[ into ::staging_schema.::curr_table]]..importFromSrc)
				local tmpRowCount = currRowCount.etl_rows_written
				wrapper:log('LOG',target_schema..'.'..target_table..' is empty -> LOAD', tmpRowCount)
			
			else -- load only new rows into the staging table

				
				-- setup parameters for (local) delta import
				if local_conn then
					importFromSrc = [[ (select * from ]]..quoteForDb(conn_db,source_schema_name)..[[.]]..quoteForDb(conn_db,target_table).. 
					[[ where ]]..quoteForDb(conn_db, delta_detection_column)..[[ > ]]..maxDelta..
					[[ )]]
				else
					-- no need to overwrite importFromSrc statement, only adapt the selectStmt
					selectDeltaStmt = selectStarStmt ..[[ where ]]..quoteForDb(conn_db, delta_detection_column)..[[ > ]]..maxDelta
					wrapper:set_param('selectStmt', selectDeltaStmt)
				end

				suc, currRowCount  = wrapper:query(insert_keyword ..[[ into ::staging_schema.::curr_table]] .. importFromSrc)
				local tmpRowCount = currRowCount.etl_rows_written
				wrapper:log('LOG',target_schema..'.'..target_table..' -> start loading at ' ..delta_detection_column..':'..maxDeltaVal[1][1], tmpRowCount)
			end
			-- data is in staging table now -> merge the staging table into the normal table based on primary keys of the original table 
			wrapper:query([[merge into ::curr_schema.::curr_table a using ::staging_schema.::curr_table b on ]]..key_columns..
					[[ when matched then update set ]]..no_key_columns..[[ when not matched then insert values (]]..all_columns..[[)  ]])

			wrapper:query('drop table ::staging_schema.::curr_table');
		end
	end
end
return wrapper:finish()
/

execute script database_migration.delta_import(
'JDBC',  			-- conn_type (JDBC / ORA/ EXA/...) To load from another schema in the same DB: set conn_type to 'LOCAL', conn_db to '' and conn_name to ''
'MYSQL',	 		-- conn_db (ORACLE, MYSQL, EXASOL...)
'MY_CONN', 			-- conn_name, case sensitive
'SOURCE_SCHEMA',	-- source_schema_name (can NOT contain wildcards), case sensitive
'TARGET_SCHEMA',  	-- target_schema_name (can contain wildcards), case sensitive
'TABLE_%',     		-- table_name (can contain wildcards), case sensitive
'LAST_UPDATED',		-- delta_detection_column case sensitive
'DELTA_STAGING' 	-- staging_schema, used for log files and temporary data storage
)
;
