--the delta_import script is designed to do a delta-load with the import statement, in order to load only new columns.
--to merge updated rows into the destination table primary keys are used and are expected for the destination table (primary keys can be deactivated / only metadata)
--in case there is no update column or primary keys -> a full load of the table is done.

--conn_type: connection type -> one of the connection types the import statement support -> e.g. JDBC (to use with a generic database), EXA, ORA
--conn_db: 	which database is this script importing from --> 'MYSQL' or 'EXASOL'
--conn_name: name of the connection -> that connection has to be created upfront and is used for the import statements
--schema_name: name of the schema -> can also include wildcards for a like-clause
--table_name: name of the table -> can include wildcard -> e.g. % to load the whole schema
--update_column: name of the column used for delta loading -> if that column exists new or modified rows have to have an increased value compared to the last load (max is used) -> e.g. use an increasing job number in the source system or a modified timestamp.
--staging_schema_name: name of a temporary schema used for storing tables into before merging them into the original tables
create or replace script database_migration.delta_import(conn_type, conn_db, conn_name, schema_name, table_name, update_column, staging_schema_name) returns table
 as
-----------------------------------------------------------------------------------------
function quoteForDb(db_name, string_to_quote)
escape_char = '"'
	if db_name == 'MYSQL' then
		escape_char = '`'
	end
	return escape_char .. string_to_quote .. escape_char
end
-----------------------------------------------------------------------------------------
function replaceQuotesForDb(db_name, string_to_requote)
escape_char = '"'
	if db_name == 'MYSQL' then
		escape_char = '`'
	end
	newString = string.gsub(string_to_requote, '"', escape_char)
	return newString
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
-----------------------------------------------------------------------------------------
-- create schema and tables for log data
import('ETL.QUERY_WRAPPER','QW')
wrapper = QW.new( 'ETL.JOB_LOG', 'ETL.JOB_DETAILS', 'delta_import_on_primary_keys')

query([[
create schema if not exists ETL
]])
query([[
create table if not exists ::s.job_log_delta_import(
			run_id int identity,
			script_name varchar(100),
			status varchar(100),
			start_time timestamp default systimestamp,
			end_time timestamp
		)
]], {s= staging_schema_name})

query([[
		create table if not exists ::s.job_details_delta_import (
			detail_id int identity,
			run_id int,
			log_time timestamp,
			log_level varchar(10),
			log_message varchar(2000),
			rowcount int
		)
]], {s= staging_schema_name})

query([[create schema if not exists ::s ]], {s= staging_schema_name})

-- setup all parameters

wrapper:set_param('schema_filter',schema_name)
wrapper:set_param('table_filter',table_name)

--query all needed information for schema name and table name within one resultset -> go through that resultset (res) afterwards
suc, res = wrapper:query([[with constr as (
		select constraint_schema, constraint_table,	COLUMN_NAME
		from EXA_ALL_CONSTRAINT_COLUMNS
		where constraint_schema LIKE :schema_filter and
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
		WHERE TABLE_SCHEMA LIKE :schema_filter and
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
	'"' || all_tables.table_schema || '"',
	'"' || all_tables.table_name || '"',
	key_columns,
	all_columns,
	no_key_columns
from tables_with_collist
	right join	all_tables on
		all_tables.table_schema = tables_with_collist.schema_name and
		all_tables.table_name = tables_with_collist.table_name
order by 1,2 asc
]])
--debug(res)

wrapper:set_param('conn_type', conn_type)
wrapper:set_param('conn_name', conn_name)
wrapper:set_param('staging_schema',staging_schema_name)

--iterate through the resultset
for i=1,#res do 

	wrapper:set_param('curr_schema', res[i][1])
	wrapper:set_param('curr_table', res[i][2])

	if res[i][3] == NULL then
		--no key column -> do a full load, no need for using the staging table
		wrapper:query([[truncate table ::curr_schema.::curr_table]]);
		suc, currRowCount = wrapper:query([[import into ::curr_schema.::curr_table from ::conn_type at ::conn_name statement 'select * from ]]..replaceQuotesForDb(conn_db,res[i][1])..[[.]]..replaceQuotesForDb(conn_db,res[i][2])..[[']])
		local tmpRowCount = currRowCount.etl_rows_written
		wrapper:log('LOG',res[i][1]..'.'..res[i][2]..' has no key column -> TRUNCATE and LOAD', tmpRowCount)

	else
		--there is a key column create a staging table and try to identify the maximum of the update column to load the delta
		wrapper:query('create or replace table ::staging_schema.::curr_table like ::curr_schema.::curr_table');
		-- TODO: Can we use the wrapper here? Don't want to throw an error if statement doesn't succeed
		local success, maxRows = pquery('select max(::col_name) from ::curr_schema.::curr_table', {col_name=quote(update_column), curr_schema=res[i][1], curr_table=res[i][2]});
		
		selectStarStmt = [[select * from ]]..replaceQuotesForDb(conn_db,res[i][1])..[[.]]..replaceQuotesForDb(conn_db,res[i][2])
		wrapper:set_param('selectStarStmt', selectStarStmt)

		if not success then
			output(maxRows.statement_text)
			--error when selecting the max value -> there seems to be no update column -> do a truncate and then a load
			wrapper:query([[truncate table ::curr_schema.::curr_table]]);
			suc, currRowCount = wrapper:query([[import into ::curr_schema.::curr_table from ::conn_type at ::conn_name statement :selectStarStmt]])
			local tmpRowCount = currRowCount.etl_rows_written
			wrapper:log('LOG',res[i][1]..'.'..res[i][2]..' has key column, but no column '..update_column..' -> TRUNCATE and LOAD', tmpRowCount)
		else
			--querying maxcol was successful -> now do a load
			if maxRows[1][1]==NULL then
				--seems to be empty -> do a full load into the staging table
				suc, currRowCount = wrapper:query([[import into ::staging_schema.::curr_table from ::conn_type at ::conn_name statement :selectStarStmt]])
				local tmpRowCount = currRowCount.etl_rows_written
				wrapper:log('LOG',res[i][1]..'.'..res[i][2]..' is empty -> LOAD', tmpRowCount)
			else
				--only load the data into the staging table
				selectDeltaStmt = [[select * from ]]..replaceQuotesForDb(conn_db, res[i][1])..[[.]]..replaceQuotesForDb(conn_db, res[i][2])..[[ where ]]..replaceQuotesForDb(conn_db, update_column)..[[ > ]]..maxRows[1][1]
				wrapper:set_param('selectDeltaStmt', selectDeltaStmt)
				wrapper:set_param('max_cur_val', maxRows[1][1])

				suc, currRowCount  = wrapper:query([[import into ::staging_schema.::curr_table from ::conn_type at ::conn_name STATEMENT :selectDeltaStmt]])
				local tmpRowCount = currRowCount.etl_rows_written
				wrapper:log('LOG',res[i][1]..'.'..res[i][2]..' has column '..update_column..' -> start loading at '..maxRows[1][1], tmpRowCount)
			end

			-- data is in staging table now -> merge the staging table into the normal table based on primary keys of the original table 
			local key_columns    = res[i][3]
			local all_columns 	 = res[i][4]
			local no_key_columns = res[i][5]
			wrapper:query([[merge into ::curr_schema.::curr_table a using ::staging_schema.::curr_table b on ]]..key_columns..
					[[ when matched then update set ]]..no_key_columns..[[ when not matched then insert values (]]..all_columns..[[)]])

			wrapper:query('drop table ::staging_schema.::curr_table');
		end
	end
	--if there are huge load jobs which might take some time uncomment the following line to do a commit within this script after loading each table -> note that 
	--wrapper:query([[commit]])
end
return wrapper:finish()
/

execute script database_migration.delta_import(
'JDBC',  		-- conn_type (JDBC / EXA/...)
'MYSQL', 		-- conn_db (ORACLE, MYSQL)
'JDBC_MYSQL', 	-- conn_name
'my_schema_%',  -- schema_name (can contain wildcards)
'%',     		-- table_name (can contain wildcards)
'id',    		-- update_column
'DELTA_STAGING' -- staging_schema_name
);
