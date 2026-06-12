CREATE SCHEMA IF NOT EXISTS DATABASE_MIGRATION;

--/
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT DATABASE_MIGRATION."AZURE_GET_FILENAMES"
(
 "connection_name" VARCHAR(1024),
 "folder_name" VARCHAR(1024) UTF8,
 "container_name" VARCHAR(1024) UTF8,
 "filter_string" VARCHAR(1024)
) 
EMITS ("CONTAINER_NAME" VARCHAR(1024) UTF8, "URL" VARCHAR(4096) UTF8, "FILE_LAST_MODIFIED" TIMESTAMP) AS
import fnmatch

from azure.storage.blob import BlobServiceClient

#####################################################################################

def run(ctx):
    con = exa.get_connection(ctx.connection_name)
    azure_account_name = con.user
    azure_account_key = con.password
    account_url = f"https://{azure_account_name}.blob.core.windows.net"
    azure_container_name = ctx.container_name
    
    blob_service_client = BlobServiceClient(account_url, credential=azure_account_key)
    
    container_client = blob_service_client.get_container_client(container=azure_container_name)
    
    blob_list = container_client.list_blobs(name_starts_with=ctx.folder_name)

    for blob in blob_list:
        if not ctx.filter_string or fnmatch.fnmatch(blob.name, ctx.filter_string):
            ctx.emit(azure_container_name, blob.name, blob.last_modified.replace(tzinfo=None))
/

select DATABASE_MIGRATION.azure_get_filenames(
'MY_BLOBSTORAGE'						-- connection_name
, '2024/' 	-- folder_name
, 'temp'			    					-- container_name
, '*2.csv'			    			-- filter_string
);


------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------

-- # parallel_connections: number of parallel files imported in one import statement
-- # file_opts:			 search EXASolution_User_Manual for 'file_opts' to see all possible options
--/
CREATE OR REPLACE LUA SCRIPT DATABASE_MIGRATION."AZURE_PARALLEL_READ"
(
 execute_statements,
 force_reload,
 logging_schema,
 schema_name,
 table_name,
 number_cols,
 connection_name,
 container_name,
 folder_name,
 filter_string,
 parallel_connections,
 file_opts) 
RETURNS TABLE AS
------------------------------------------------------------------------------------------------------------------------

-- function for debugging, prints table
function debug(table)
	if table == nil or table[1] == nil or table[1][1] == nil then
		return
	end

	output(string.rep('-',50))
	for i = 1,#table do
		tmp = ''
		for j = 1,#table[i] do
			tmp = tmp .. table[i][j] ..'	'
		end
		output(tmp)
	end
	output(string.rep('-',50))
end

------------------------------------------------------------------------------------------------------------------------
-- type: 'table' or 'schema'
-- name: name of the table/ schema you want to check
-- returns true if object exists, false if it doesn't exist, second return parameter is error message you can output
function check_exists(type,name)

	-- schema
	if type == 'schema' or type == 's' then
		res = query([[select current_schema]])
		curr_schem = res[1][1]
		suc_schem, res = pquery([[open schema ::s]], {s=name})
		-- restore old schema
		if curr_schem == null then
			query([[close schema]])
		else
			query([[open schema ::cs]],{cs=curr_schem})
		end
		-- check if schema named 'name' existed
		if suc_schem then 
			return true, 'Schema '..name..' already exists'
		else
		 	return false, 'Schema '..name..' does not exist'
		end
	end

	-- table  for table names you should add the schema
	-- like this: check_name('table', 'my_schema.my_table')
	if type == 'table' or type == 't' then
		suc, res = pquery([[desc ::t]],{t=name})
		if suc then 
			return true, 'Table '..name..' already exists'
		else
		 	return false, 'Table '..name..' does not exist'
		end
	end
end

------------------------------------------------------------------------------------------------------------------------

	-- additional parameters that won't need to be modified normally
	script_schema = quote(exa.meta.script_schema)

	-- quote everything to handle case sensitivity
	logging_schema = quote(logging_schema)
	schema_name    = quote(schema_name)
	table_name     = quote(table_name)

	ex, msg = check_exists('schema', logging_schema)
	if not ex then error(msg) end

	ex, msg = check_exists('table', schema_name..'.'..table_name)
	if not ex then error(msg) end


	status_done				= 'done'
	waiting_for_update 		= 'waiting for update'
	waiting_for_insertion 	= 'waiting for insertion'

	local log_tbl = {}
	local inserted_total = 0

	-- acquire write lock on the table to prevent transactioin conflicts
	query([[DELETE FROM ::s.::t WHERE FALSE]], {s=schema_name, t=table_name})

    -- create a regular name by removing special characters
	logging_table = [["LOG_]].. string.gsub(schema_name, "[^a-zA-Z0-9]", "") .. [[_]] .. string.gsub(table_name, "[^a-zA-Z0-9]", "")..[["]]

	
	query([[CREATE TABLE IF NOT EXISTS ::s.::t (container_name varchar(20000), file_name varchar(20000), FILE_LAST_MODIFIED timestamp, status varchar(20000), FILE_LAST_TRIED timestamp)]],
		{s=logging_schema, t=logging_table})

	if(force_reload) then
		trun = query([[TRUNCATE TABLE ::ls.::lt]],{ls= logging_schema, lt= logging_table})
		table.insert(log_tbl,{'Truncated '..logging_schema..'.'..logging_table, trun.rows_affected ,trun.statement_text, ''})

		trun = query([[TRUNCATE TABLE ::s.::t]],{s= schema_name, t= table_name})
		table.insert(log_tbl,{'Truncated '..schema_name..'.'..table_name,trun.rows_affected ,trun.statement_text, ''})
	end


	-- update logging table: for all new files, add an entry
	-- for all existing files, update FILE_LAST_MODIFIED column and status column
	query([[
		merge into ::ls.::lt as l using 
		( select ::ss.azure_get_filenames(:c,:fn, :cont, :fi) order by 1) as p
		on p.url = l.file_name and p.container_name = l.container_name
		WHEN MATCHED THEN UPDATE SET l.status = :wu, l.FILE_LAST_MODIFIED = p.FILE_LAST_MODIFIED where p.FILE_LAST_MODIFIED > l.FILE_LAST_MODIFIED or status not = :sd
		WHEN NOT MATCHED THEN INSERT (container_name, file_name, FILE_LAST_MODIFIED, status) VALUES (p.container_name, p.url, p.FILE_LAST_MODIFIED, :wi);
	]], {ss=script_schema, c=connection_name, fn=folder_name, fi=filter_string, cont=container_name, ls=logging_schema, lt=logging_table, sd=status_done, wu=waiting_for_update, wi=waiting_for_insertion})


	-- get the container name and the file names of the files that should be modified
    local res = query([[
			select * from ::ls.::lt where status like 'waiting%'   
    ]], {ls=logging_schema, lt=logging_table})


	if(#res == 0) then
		exit({{'No queries generated, either container is empty or files have already been imported'}}, "message varchar(2000000)") 
	end
	
	-- generate query text for parallel import
    local queries = {}
    local stmt    = ''
	local blobs = ''
    local pre     = "IMPORT INTO ".. schema_name.. ".".. table_name .. " FROM CSV AT CLOUD AZURE BLOBSTORAGE "..connection_name

    for i = 1, #res do
		local curr_file_name 	      = "'" .. res[i].FILE_NAME .. "'"
		local curr_full_file_name 	      = "'" .. res[i].CONTAINER_NAME .. "/" .. res[i].FILE_NAME .. "'"
		
        if math.fmod(i,parallel_connections) == 1 or parallel_connections == 1 then
            stmt = pre
        end
        if number_cols == NULL then
			file_range = ' '
        elseif(number_cols == 1) then
			file_range = ' (1)'
		else
			file_range = ' (1..'..number_cols..')'
        end
        stmt = stmt .. "\n\tFILE " .. curr_full_file_name .. file_range
		blobs = blobs ..curr_file_name..", "
        if (math.fmod(i,parallel_connections) == 0 or i == #res) then
            stmt = stmt .. "\n\t"..file_opts..";"
			-- remove the last comma from blobs
			blobs = string.sub(blobs, 0, #blobs -2)
            table.insert(queries,{stmt, container_name, blobs})
			blobs = ''
        end
    end

	-- if statements should be only generated, end program here
	if not execute_statements then
		exit(queries, "queries varchar(2000000)")
	end

	for i = 1, #queries do
		-- execute query
		curr_query  = queries[i][1]
		curr_container = queries[i][2]
		curr_files  = queries[i][3]
		suc, res    = pquery(curr_query)

		if not suc then
			output(res.statement_text)
			status_error = 'Error: '.. res.error_message
			query([[update ::ls.::lt set status = :se, FILE_LAST_TRIED = systimestamp
				    where file_name in (]]..curr_files..[[) and container_name = :c
			]],{ls=logging_schema, lt=logging_table, se=status_error, c=curr_container})
			table.insert(log_tbl,{'Error while inserting: '.. res.error_message,0,curr_query, curr_files})
		else
			query([[update ::ls.::lt set status = :sd, FILE_LAST_TRIED = systimestamp
				    where file_name in (]]..curr_files..[[) and container_name = :c
			]],{ls=logging_schema, lt=logging_table, sd = status_done, c=curr_container})
			inserted_total = inserted_total + res.rows_inserted
			table.insert(log_tbl,{'Inserted', res.rows_inserted,curr_query, curr_files})
		end
	end

	table.insert(log_tbl, {'Summary, total inserts:', inserted_total, '', ''})
	exit(log_tbl, "status varchar(20000), affected_rows decimal(18,0) ,executed_queries varchar(2000000),files  varchar(2000000)")
/

CREATE CONNECTION MY_BLOBSTORAGE
TO 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net'
USER '<my_account_name>' 
IDENTIFIED BY '<my_account_key>';

create schema AZURE_LOADER_LOGGING;

-- USE CONNECTION MY_BLOBSTORAGE
execute script DATABASE_MIGRATION.azure_parallel_read(
  true				-- execute_statements: if true, statements are executed immediately, if false only statements are generated
, false				-- force reload: if true, target table and logging table will be truncated, all files will be loaded again
, 'AZURE_LOADER_LOGGING'		-- schema you want to use for the logging tables
, 'TEST'			-- name of the schema that holds the table you want to import into
, 'AZURE_TARGET' 			-- name of the table you want to import into
, NULL				-- number_cols: NULL if you want to import all columns of the file, if set to a number n, the first n  columns will be imported
, 'MY_BLOBSTORAGE'		-- connection name ( see statement above)
, 'temp'				-- Azure Blob Storage container name
, '2024/' 			-- folder name (no regex!), if you want to import everything, leave blank
, '*.csv'        	 	-- filter for file-names, to include all files, put empty string, example for filter '*_2018.csv'
, 4 				-- number of parallel connections you want to use
, 'ENCODING=''UTF-8'' SKIP=0  ROW SEPARATOR = ''LF'' COLUMN SEPARATOR = '',''' -- file options, see manual, section 'import' for further details, set skip if you have a headers
)
;