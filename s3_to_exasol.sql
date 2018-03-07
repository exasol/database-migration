create schema if not exists database_migration;
open schema database_migration;

CREATE OR REPLACE PYTHON SCALAR SCRIPT database_migration."S3_CONNECTION_HELPER" () RETURNS BOOLEAN AS
import sys
import glob
# the package boto needs to be installed via EXAoperation first
sys.path.extend(glob.glob('/buckets/bucketfs1/python/*'))
import boto
import boto.s3.connection
if not boto.config.get('s3', 'use-sigv4'):
    boto.config.add_section('s3')
    boto.config.set('s3', 'use-sigv4', 'True')
# enter your aws credentials here
s3conn = boto.s3.connection.S3Connection('<aws access key>', '<aws secret key>', host=boto.s3.connection.S3Connection.DefaultHost)
/

CREATE OR REPLACE PYTHON SCALAR SCRIPT database_migration."S3_GENERATE_URLS" ("force_http" BOOLEAN, "bucket_name" VARCHAR(1024) UTF8, "folder_name" VARCHAR(1024) UTF8) EMITS ("URL" VARCHAR(4096) UTF8) AS
import sys
import glob
# the package boto needs to be installed via EXAoperation first
sys.path.extend(glob.glob('/buckets/bucketfs1/python/*'))
import boto
s3conn = exa.import_script('database_migration.s3_connection_helper').s3conn
def run(ctx):
    bucket = s3conn.get_bucket(ctx.bucket_name, validate=False)
    rs = bucket.list(ctx.folder_name)
    for key in rs:
        # http://stackoverflow.com/questions/9954521/s3-boto-list-keys-sometimes-returns-directory-key
        if not key.name.endswith('/'):
            # expires_in defines the expiry of the url. It has only an effect if query_auth=True. With value True, a signature is created.
            # query_auth can also be set to False, if it is not required. Then, no signature is created.
            # protocol, filepath = key.generate_url(expires_in=3600,force_http=ctx.force_http,query_auth=True).split('://', 1)
            protocol, filepath = key.generate_url(expires_in=0,force_http=ctx.force_http,query_auth=False).split('://', 1)
            s3_bucket, localpath = filepath.split('/', 1)
            ctx.emit(localpath)
/

-- parallel_connections: number of parallel files imported in one import statement
-- file_opts:			 search EXASolution_User_Manual for 'file_opts' to see all possible options
CREATE OR REPLACE LUA SCRIPT database_migration.S3_PARALLEL_READ(table_name,bucket_name,folder_name, parallel_connections, file_opts) RETURNS TABLE AS
force_http = true
    
    local par = 4
    local pre = "IMPORT INTO " .. table_name .. " FROM CSV AT S3 "
    -- if force_http then pre = pre .. "'http://'" else pre = pre .. "'https://'" end
    local res = query([[
        select database_migration.s3_generate_urls(:fh, :bn, :fn) order by 1
    ]], {fh=force_http, bn=bucket_name, fn=folder_name})
    local fin = {}
    local str = ''
    for i = 1, #res do
        if math.fmod(i,parallel_connections) == 1 or parallel_connections == 1 then
            str = pre
        end
        str = str .. "\n\tFILE '" .. res[i][1] .. "'"
        if (math.fmod(i,parallel_connections) == 0 or i == #res) then
            str = str .. "\n\t"..file_opts..";"
            table.insert(fin,{str})
        end
    end
    exit(fin, "queries varchar(2000000)")
/


select database_migration.s3_generate_urls(true, 'big-data-benchmark','pavlo/text/tiny/rankings/') order by 1;

execute script database_migration.s3_parallel_read('my_table','big-data-benchmark','pavlo/text/tiny/rankings/',2,'ENCODING=''ASCII'' SKIP=1 REJECT LIMIT UNLIMITED');
