create schema load_metadata;

/* This script will generate create schema, create table and create import statements to load all needed data from a postgres database. Automatic datatype conversion is applied whenever needed. Feel free to adjust it. */
create or replace script load_metadata.LOAD_FROM_MYSQL(
CONNECTION_NAME --name of the database connection inside exasol -> e.g. mysql_db
,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
,SCHEMA_FILTER --filter for the schemas to generate and load (except information_schema and pg_catalog) -> '%' to load all
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
with vv_mysql_columns as (
	select ]]..exa_upper_begin..[[table_catalog]]..exa_upper_end..[[ as "exa_table_catalog", ]]..exa_upper_begin..[[table_schema]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[[table_name]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", mysql.* from  
		(import from jdbc at ]]..CONNECTION_NAME..[[ statement 
			'select table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, column_type, character_maximum_length, numeric_precision, numeric_scale  
				from information_schema.columns join information_schema.tables using (table_catalog, table_schema, table_name) 
				where table_type = ''BASE TABLE'' 
				AND table_schema not in (''information_schema'',''performance_schema'', ''mysql'')
				AND table_schema like '']]..SCHEMA_FILTER..[[''
				AND table_name like '']]..TABLE_FILTER..[[''
		') as mysql 
)
,vv_create_schemas as(
	SELECT 'create schema "' || "exa_table_schema" || '";' as sql_text from vv_mysql_columns  group by "exa_table_catalog","exa_table_schema" order by "exa_table_catalog","exa_table_schema"
)
,vv_create_tables as (
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' ||
	case 
    -- ### numeric types ###
    when upper(data_type) = 'INT' then 'DECIMAL(11,0)'
    when upper(data_type) = 'INTEGER' then 'DECIMAL(11,0)'
    when upper(data_type) = 'TINYINT' then 'DECIMAL(4,0)'
    when upper(data_type) = 'SMALLINT' then 'DECIMAL(5,0)'
    when upper(data_type) = 'MEDIUMINT' then 'DECIMAL(9,0)'
    when upper(data_type) = 'BIGINT' then 'DECIMAL (20,0)'
    when upper(data_type) = 'FLOAT' then 'FLOAT'
    when upper(data_type) = 'DOUBLE' then 'DOUBLE'   
    when upper(data_type) = 'DECIMAL' then case when numeric_precision is null or numeric_precision > 36 then 'DOUBLE' else 'decimal(' || numeric_precision || ',' || case when (numeric_scale > numeric_precision) then numeric_precision else  case when numeric_scale < 0 then 0 else numeric_scale end end || ')' end 
    when upper(data_type) = 'BIT' then 'DECIMAL('||numeric_precision||',0)'
    -- ### date and time types ###
    when upper(data_type) = 'DATE' then 'DATE'
    when upper(data_type) = 'DATETIME' then 'TIMESTAMP'
    when upper(data_type) = 'TIMESTAMP' then 'TIMESTAMP'
    when upper(data_type) = 'TIME' then 'varchar(8)'
    when upper(data_type) = 'YEAR' then 'varchar(4)'
    -- ### string types ###
    when upper(data_type) = 'CHAR' then upper(column_type)
    when upper(data_type) = 'VARCHAR' then upper(column_type)
    when upper(data_type) = 'BINARY' then 'char('||character_maximum_length||')'
    when upper(data_type) = 'VARBINARY' then 'varchar('||character_maximum_length||')'
    when upper(data_type) = 'TEXT' then 'varchar(2000000)'
    when upper(data_type) = 'BLOB' then 'varchar(2000000)'
    when upper(data_type) = 'ENUM' then 'varchar(2000000)'
    when upper(data_type) = 'SET' then 'varchar(2000000)'

    -- ### fallback for unknown types ###
	else '/*UNKNOWN_DATATYPE:' || data_type || '*/ varchar(2000000)' end
	order by ordinal_position) || ');' as sql_text
	from vv_mysql_columns  group by "exa_table_catalog","exa_table_schema", "exa_table_name"
	order by "exa_table_catalog","exa_table_schema","exa_table_name"
)
, vv_imports as (
	select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' 
           || group_concat(
                           case 
	                       when upper(data_type) = 'BINARY' then 'cast(`'||column_name||'` as char('||character_maximum_length||'))'
                           when upper(data_type) = 'VARBINARY' then 'cast(`'||column_name||'` as char('||character_maximum_length||'))'
                           when upper(data_type) = 'BLOB' then 'cast(`'||column_name||'` as char(2000000))'
                           else '`' || column_name || '`' end order by ordinal_position) 
           || ' from ' || table_schema|| '.' || table_name|| ''';' as sql_text
	from vv_mysql_columns group by "exa_table_catalog","exa_table_schema","exa_table_name", table_schema,table_name
	order by "exa_table_catalog", "exa_table_schema","exa_table_name", table_schema,table_name
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


create or replace connection mysql_conn 
to 'jdbc:mysql://192.168.137.5:3306'
user 'user'
identified by 'exasolRocks!';

execute script load_metadata.LOAD_FROM_MYSQL('mysql_conn' --name of your database connection
,TRUE -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
,'mb%' -- schema filter --> '%' to load all schemas except 'information_schema' and 'mysql' and 'performance_schema' / '%publ%' to load all schemas like '%pub%'
,'%' -- table filter --> '%' to load all tables (
);

