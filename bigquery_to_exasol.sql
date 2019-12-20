create schema if not exists database_migration;

/* 
	This script will generate create schema, create table and create import statements 
	to load all needed data from Google bigquery. Automatic datatype conversion is
	applied whenever needed. Feel free to adjust it. 
*/
--/

create or replace script database_migration.BIGQUERY_TO_EXASOL(
    CONNECTION_NAME              -- name of the database connection inside exasol -> e.g. bigquery_db
    ,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
    ,PROJECT_ID                  -- name of bigquery project 
    ,SCHEMA_FILTER               -- filter for the schemas to generate and load -> '%' to load all
    ,TABLE_FILTER                -- filter for the tables to generate and load -> '%' to load all
) RETURNS TABLE
AS
exa_upper_begin=''
exa_upper_end=''
stat = ''

if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

if SCHEMA_FILTER == '%' then 
	suc, res = pquery([[SELECT * FROM (IMPORT FROM JDBC AT ]]..CONNECTION_NAME..[[ STATEMENT 'select * from INFORMATION_SCHEMA.SCHEMATA')]])
	for i=1, #res do
		schema_name = res[i][2]
		stat = stat..[[statement]]..[[ 
			'select  TABLE_CATALOG,TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION ,IS_NULLABLE, DATA_TYPE   
					from `]]..PROJECT_ID ..[[`.]]..schema_name..[[.INFORMATION_SCHEMA.COLUMNS join `]]..PROJECT_ID ..[[`.]]..schema_name..[[.INFORMATION_SCHEMA.TABLES using (table_catalog, table_schema, table_name) 
					where table_type = ''BASE TABLE'' 
					AND table_schema not in (''INFORMATION_SCHEMA'')
					AND table_schema like '']]..schema_name..[[''
					AND table_name like '']]..TABLE_FILTER..[[''
					']]        
	end
else 
stat = stat..[[statement]]..[[ 
			'select  TABLE_CATALOG,TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION ,IS_NULLABLE, DATA_TYPE   
					from `]]..PROJECT_ID ..[[`.]]..SCHEMA_FILTER..[[.INFORMATION_SCHEMA.COLUMNS join `]]..PROJECT_ID ..[[`.]]..SCHEMA_FILTER..[[.INFORMATION_SCHEMA.TABLES using (table_catalog, table_schema, table_name) 
					where table_type = ''BASE TABLE'' 
					AND table_schema not in (''INFORMATION_SCHEMA'')
					AND table_schema like '']]..SCHEMA_FILTER..[[''
					AND table_name like '']]..TABLE_FILTER..[[''
					']]    
end

suc, res = pquery([[

with vv_bigquery_columns as (
	select ]]..exa_upper_begin..[[table_catalog]]..exa_upper_end..[[ as "exa_table_catalog", ]]..exa_upper_begin..[[table_schema]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[[table_name]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", bigquery.* from  
		(import from jdbc at ]]..CONNECTION_NAME..[[ ]]..stat..[[) as bigquery 
)

,vv_create_schemas as(
	SELECT 'create schema if not exists "' || "exa_table_schema" || '";' as sql_text from vv_bigquery_columns  group by "exa_table_catalog","exa_table_schema" order by "exa_table_catalog","exa_table_schema"
)

,vv_create_tables as (
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat(
	case 
    -- ### numeric types ###
    when upper(data_type) = 'INT64' then '"' || "exa_column_name" || '" ' || 'DECIMAL(19,0)'
    when upper(data_type) = 'NUMERIC' then '"' || "exa_column_name" || '" ' || 'VARCHAR(50)'
    when upper(data_type) = 'FLOAT64' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'

    -- ### date and time types ###
    when upper(data_type) = 'DATE' then '"' || "exa_column_name" || '" ' || 'DATE'
    when upper(data_type) = 'DATETIME' then '"' || "exa_column_name" || '" ' || 'VARCHAR(30)'
    when upper(data_type) = 'TIMESTAMP' then '"' || "exa_column_name" || '" ' || 'VARCHAR(30)'
    when upper(data_type) = 'TIME' then '"' || "exa_column_name" || '" ' || 'VARCHAR(16)'

    -- ### string types ###
	when upper(data_type) = 'STRING' then '"' || "exa_column_name" || '" ' || 'VARCHAR(2000000)'

    --- ### boolean data type ###		
	 when upper(data_type) = 'BOOL' then '"' || "exa_column_name" || '" ' || 'BOOLEAN'

	-- ### geospatial types ###	
	when upper(data_type) = 'GEOGRAPHY' then '"' || "exa_column_name" || '" ' || 'GEOMETRY(4326)'

	---### other data types ###
    when upper(data_type) = 'BYTES' then '"' || "exa_column_name" || '" ' || 'VARCHAR(2000000)'
	when upper(data_type) LIKE 'ARRAY%' then '"' || "exa_column_name" || '" ' || 'VARCHAR(2000000)'
	when upper(data_type) LIKE 'STRUCT%' then '"' || "exa_column_name" || '" ' || 'VARCHAR(2000000)'	

    end
	order by ordinal_position) || ');' 

	-- ### unknown types ###
	|| group_concat (
	       case 
	       when upper(data_type) NOT LIKE 'ARRAY%' AND upper(data_type) NOT LIKE 'STRUCT%' AND upper(data_type) not IN ('INT64', 'NUMERIC', 'FLOAT64', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'STRING', 'BOOL' , 'GEOGRAPHY' , 'BYTES') 
	       then '--UNKNOWN_DATATYPE: "'|| "exa_column_name" || '" ' || upper(data_type) || ''
	       end
	)|| ' 'as sql_text
	from vv_bigquery_columns  group by "exa_table_catalog","exa_table_schema", "exa_table_name"
	order by "exa_table_catalog","exa_table_schema","exa_table_name"
)

, vv_imports as (
	select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' 
           || group_concat(
							case
							-- ### numeric types ###
							when upper(data_type) = 'INT64' then '`' || column_name || '`' 
							when upper(data_type) = 'FLOAT64' then '`' || column_name || '`' 
							WHEN upper(data_type) = 'NUMERIC' then '`' ||column_name || '`'

							-- ### date and time types ###
							when upper(data_type) = 'DATE' then '`' || column_name || '`' 
							when upper(data_type) = 'DATETIME' then '`' || column_name || '`' 
							when upper(data_type) = 'TIMESTAMP' then '`' || column_name || '`' 

							when upper(data_type) = 'TIME' then '`' || column_name || '`' 
							
							-- ### string types ###
							when upper(data_type) = 'STRING' then '`' || column_name || '`' 
							when upper(data_type) = 'GEOGRAPHY' then '`'||column_name||'`'
						
							--- ### boolean data type ###		
							 when upper(data_type) = 'BOOL' then '`' || column_name || '`'

							---### other data types###
						    when upper(data_type) = 'BYTES' then '`' || column_name || '`'
							when upper(data_type) LIKE 'ARRAY%' then '`' || column_name || '`'
							when upper(data_type) LIKE 'STRUCT%' then '`' || column_name || '`'	
													
							end order by ordinal_position) 
           || ' from ' || table_schema|| '.' || table_name|| ''';' as sql_text
	from vv_bigquery_columns group by "exa_table_catalog","exa_table_schema","exa_table_name", table_schema,table_name
	order by "exa_table_catalog", "exa_table_schema","exa_table_name", table_schema,table_name
)

select SQL_TEXT from (
select 1 as ord, cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
union all 
select 2, a.* from vv_create_schemas a
union all 
select 3, cast('-- ### TABLES ###' as varchar(2000000)) SQL_TEXT
union all
select 4, b.* from vv_create_tables b
WHERE b.SQL_TEXT NOT LIKE '%();%'
union all
select 5, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select 6, c.* from vv_imports c
WHERE c.SQL_TEXT NOT LIKE '%select  from%' 
) order by ord
]],{})

if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return(res)
/

-- !!! Important: Before creating a Google Bigquery connection, follow the steps outlined here: https://docs.exasol.com/loading_data/connect_databases/google_bigquery.htm !!!

-- Create a connection to the Google BigQuery
CREATE CONNECTION BQ_MIGRATE TO 'jdbc:exaquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=exa-migration;OAuthType=0;Timeout=10000;OAuthServiceAcctEmail=migration-test@exa-migration.iam.gserviceaccount.com;OAuthPvtKeyPath=/d02_data/bfsdefault/bqmigration/my-key.json;';

-- Finally start the import process
execute script database_migration.BIGQUERY_TO_EXASOL(
    'BQ_MIGRATE'           -- name of the database connection inside exasol -> e.g. bigquery_db
    ,False                 -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
    ,'bigquerymigration'   -- name of bigquery project
    ,'%'                   -- filter for the schemas to generate and load -> '%' to load all
    ,'%'                   -- filter for the tables to generate and load -> '%' to load all
);