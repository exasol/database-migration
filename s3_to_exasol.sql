create schema if not exists database_migration;
open schema database_migration;


CREATE OR REPLACE PYTHON SCALAR SCRIPT database_migration."S3_GET_FILENAMES" ("force_http" BOOLEAN, "connection_name" VARCHAR(1024), "folder_name" VARCHAR(1024) UTF8, "generate_urls" BOOLEAN) 
EMITS ("BUCKET_NAME" VARCHAR(1024) UTF8, "URL" VARCHAR(4096) UTF8, "LAST_MODIFIED" TIMESTAMP) AS
import sys
import glob
# the package boto needs to be installed via EXAoperation first
sys.path.extend(glob.glob('/buckets/bucketfs1/python/*'))
import boto
import boto.s3.connection


#####################################################################################
def s3_connection_helper(connection_name):
    con = exa.get_connection(connection_name)
    aws_key = con.user
    aws_secret = con.password
    address = con.address

    bucket_pos = address.find('://') + 3
    host_pos = address.find('.s3.') + 1

    bucket_name = address[bucket_pos:host_pos-1]
    host_address = address[host_pos:]

    # use sigv4: modify the connection to use the credentials provided below
    if not boto.config.get('s3', 'use-sigv4'):
        boto.config.add_section('s3')
        boto.config.set('s3', 'use-sigv4' , 'True')
    
    if aws_key == '':
        s3conn = boto.s3.connection.S3Connection(host=host_address)
    else:
        s3conn = boto.s3.connection.S3Connection(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, host=host_address)
    return s3conn, bucket_name;



#####################################################################################

def run(ctx):
    
    s3conn, bucket_name = s3_connection_helper(ctx.connection_name)
    bucket = s3conn.get_bucket(bucket_name)
    rs = bucket.list(prefix=ctx.folder_name)
    for key in rs:
        # http://stackoverflow.com/questions/9954521/s3-boto-list-keys-sometimes-returns-directory-key
        if not key.name.endswith('/'):
            if ctx.generate_urls:
                # expires_in: defines the expiry of the url in seconds. It has only an effect if query_auth=True. With value True, a signature is created.
                # query_auth: can also be set to False, if it is not required. Then, no signature is created.
                protocol, filepath = key.generate_url(expires_in=3600,force_http=ctx.force_http,query_auth=True).split('://', 1)
                s3_bucket, localpath = filepath.split('/', 1)
            else:
                localpath = key.name
            
            ctx.emit(bucket_name, localpath, boto.utils.parse_ts(key.last_modified))
/

------------------------------------------------------------------------------------------------------------------------

-- # parallel_connections: number of parallel files imported in one import statement
-- # file_opts:			 search EXASolution_User_Manual for 'file_opts' to see all possible options
CREATE OR REPLACE LUA SCRIPT database_migration.S3_PARALLEL_READ(execute_statements, connection_name,table_name,folder_name, parallel_connections, file_opts, force_http, generate_urls) RETURNS TABLE AS
	
    local pre = "IMPORT INTO " .. table_name .. " FROM CSV AT "..connection_name

    local res = query([[
         select database_migration.s3_get_filenames(:fh,:c,:fn, :gu) order by 1
    ]], {fh=force_http, c=connection_name, fn=folder_name, gu=generate_urls})
	
    local queries = {}
    local stmt = ''
	local s3_keys = ''


    for i = 1, #res do
        if math.fmod(i,parallel_connections) == 1 or parallel_connections == 1 then
            stmt = pre
        end
        stmt = stmt .. "\n\tFILE '" .. res[i][2] .. "'"
		s3_keys = s3_keys ..res[i][2]..", "
        if (math.fmod(i,parallel_connections) == 0 or i == #res) then
            stmt = stmt .. "\n\t"..file_opts..";"
			-- remove the last comma from s3_keys
			s3_keys = string.sub(s3_keys, 0, #s3_keys -2)
            table.insert(queries,{stmt, s3_keys})
			s3_keys = ''
        end
    end

	-- if statements should be only generated, end program here
	if not execute_statements then
		exit(queries, "queries varchar(2000000)")
	end

	local log_tbl = {}
	for i = 1, #queries do
		-- execute query
		suc, res = pquery(queries[i][1])
		if not suc then
			output(res.statement_text)
			table.insert(log_tbl,{'Error while inserting: '.. res.error_message,queries[i][2],queries[i][i]})
		else	
			table.insert(log_tbl,{'Inserted',queries[i][2], queries[i][1]})
		end
		
	end

	exit(log_tbl, "status varchar(200),files varchar(20000), executed_queries varchar(2000000)")
/


execute script database_migration.s3_parallel_read(
true						-- if true, statement is directly executed, if false only the text is generated
,'S3_DEST'					-- connection name
,'database_migration.test' 	-- schema and table name
,'' 						-- folder name
,2 							-- parallel connections
,'ENCODING=''ASCII'' SKIP=1  ROW SEPARATOR = ''CRLF''' -- file options
, true 						-- If true, use http instead of https
, false						-- If true, urls are generated to access the S3 storage, if false, only key names are used
);

