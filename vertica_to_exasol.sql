create schema if not exists database_migration;

/* 
    This script will generate create schema, create table and create import statements 
    to load all needed data from a Vertica database. Automatic datatype conversion is 
    applied whenever needed. Feel free to adjust it. 
*/
--/
create or replace script database_migration.VERTICA_TO_EXASOL(
CONNECTION_NAME --name of the database connection inside exasol -> e.g. VERTICA
,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
,SCHEMA_FILTER --filter for the schemas to generate and load -> '%' to load all
,TABLE_FILTER --filter for the tables to generate and load -> '%' to load all
) RETURNS TABLE
AS
exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end
suc, res = pquery([[

with vv_vertica_columns as (

	select ]]..exa_upper_begin..[[table_schema]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[[table_name]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", vertica.* from  
		(

import  into 

(
table_schema	varchar(128),
table_name	varchar(128),
column_name	varchar(128),
data_type	varchar(128),
data_type_id	int,
data_type_length	int,
character_maximum_length	int,
numeric_precision	int,
numeric_scale	int,
datetime_precision	int,
interval_precision	int,
ordinal_position	int,
is_nullable	boolean,
column_default	varchar(65000),
is_identity	boolean
)

from jdbc at ]]..CONNECTION_NAME..[[ statement 

'select 
table_schema, 
table_name, 
column_name, 
data_type, 
data_type_id, 
data_type_length, 
character_maximum_length, 
numeric_precision, 
numeric_scale, 
datetime_precision, 
interval_precision, 
ordinal_position, 
is_nullable, 
column_default, 
is_identity

from v_catalog.columns

where is_system_table is false
AND table_schema ilike '']]..SCHEMA_FILTER..[[''
AND table_name ilike '']]..TABLE_FILTER..[[''

'

) as vertica 

)
,vv_create_schemas as(
	SELECT 'create schema "' || "exa_table_schema" || '";' as sql_text from vv_vertica_columns  group by "exa_table_schema" order by "exa_table_schema"
)
,vv_create_tables as (
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' ||
	case 
    -- ### numeric types ###
    when upper(data_type) = 'INT' then 'DECIMAL(11,0)'
    when upper(data_type) = 'INTEGER' then 'DECIMAL(11,0)'
	when upper(data_type) = 'INT8' then 'DECIMAL(11,0)'
    when upper(data_type) = 'TINYINT' then 'DECIMAL(4,0)'
    when upper(data_type) = 'SMALLINT' then 'DECIMAL(5,0)'
    when upper(data_type) = 'BIGINT' then 'DECIMAL (20,0)'
    when upper(data_type) like 'FLOAT%' then 'FLOAT'
    when upper(data_type) = 'DOUBLE PRECISION' then 'DOUBLE'   
	when upper(data_type) = 'REAL' then 'FLOAT'   
    when upper(data_type) in ('DECIMAL','NUMERIC','NUMBER') then case when numeric_precision is null or numeric_precision > 36 then 'DOUBLE' else 'decimal(' || numeric_precision || ',' || case when (numeric_scale > numeric_precision) then numeric_precision else  case when numeric_scale < 0 then 0 else numeric_scale end end || ')' end 
    when upper(data_type) = 'BOOLEAN' then 'BOOLEAN'
    -- ### date and time types ###
    when upper(data_type) = 'DATE' then 'DATE'
    when upper(data_type) = 'DATETIME' then 'TIMESTAMP'
	when upper(data_type) = 'SMALLDATETIME' then 'TIMESTAMP'
    when upper(data_type) = 'TIMESTAMP' then 'varchar(14)'
    when upper(data_type) = 'TIME' then 'varchar(8)'
    when upper(data_type) = 'YEAR' then 'varchar(4)'
    -- ### string types ###
    when upper(data_type) LIKE 'CHAR%' then upper(data_type)
    when upper(data_type) LIKE 'VARCHAR%' then upper(data_type)
	when upper(data_type) LIKE 'LONG VARCHAR%' then upper(data_type)
    when upper(data_type) = 'BINARY' then 'char('||character_maximum_length||')'
	when upper(data_type) = 'BYTEA' then 'char('||character_maximum_length||')'
	when upper(data_type) = 'RAW' then 'char('||character_maximum_length||')'
    when upper(data_type) = 'VARBINARY' then 'varchar('||character_maximum_length||')'
    when upper(data_type) = 'LONG VARBINARY' then 'varchar(2000000)'

    -- ### fallback for unknown types ###
	else '/*UNKNOWN_DATATYPE:' || data_type || '*/ varchar(2000000)' end
	order by ordinal_position) || ');' as sql_text
	from vv_vertica_columns  group by "exa_table_schema", "exa_table_name"
	order by "exa_table_schema","exa_table_name"
)
, vv_imports as (
	select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' 
           || group_concat(
                           case 
	                       when upper(data_type) = 'BINARY' then 'cast('||column_name||' as char('||character_maximum_length||'))'
                           when upper(data_type) = 'VARBINARY' then 'cast('||column_name||' as char('||character_maximum_length||'))'
                           when upper(data_type) = 'BLOB' then 'cast('||column_name||' as char(2000000))'
                           else column_name end order by ordinal_position) 
           || ' from ' || table_schema|| '.' || table_name|| ''';' as sql_text
	from vv_vertica_columns group by "exa_table_schema","exa_table_name", table_schema,table_name
	order by  "exa_table_schema","exa_table_name", table_schema,table_name
)
select cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
union all 
select * from vv_create_schemas
UNION ALL
select cast('-- ### TABLES ###' as varchar(2000000)) SQL_TEXT
union all
select * from vv_create_tables
UNION ALL
select cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select * from vv_imports]],{})

if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return(res)
/

-- !!! Important: Please upload the Vertica JDBC-Driver via EXAOperation (Webinterface) !!!
-- !!! you can see a similar example for Oracle here: https://www.exasol.com/support/browse/SOL-179 !!!

-- Create a connection to the Vertica database
create connection VERTICA_CONNECTION to 'jdbc:vertica://VerticaHostOrIP:portNumber/databaseName' user 'username' identified by 'exasolRocks!';

-- Finally start the import process
execute script 
database_migration.VERTICA_TO_EXASOL(
    'VERTICA_CONNECTION'   -- name of your database connection
    ,TRUE       -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
    ,'%'        -- schema filter --> '%' to load all schemas except system tables / '%publ%' to load all schemas like '%pub%'
    ,'%'        -- table filter --> '%' to load all tables 
);

commit;
