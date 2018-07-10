create schema database_migration;

/* 
    This script will generate create schema, create table and create import statements 
    to load all needed data from a postgres database. Automatic datatype conversion is 
    applied whenever needed. Feel free to adjust it. 
*/
create or replace script database_migration.POSTGRES_TO_EXASOL(
CONNECTION_NAME              -- name of the database connection inside exasol -> e.g. postgres_db
,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
,SCHEMA_FILTER               -- filter for the schemas to generate and load (except information_schema and pg_catalog) -> '%' to load all
,TABLE_FILTER                -- filter for the tables to generate and load -> '%' to load all
) RETURNS TABLE
AS
exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end
res = query([[
with vv_pg_columns as (
	select ]]..exa_upper_begin..[["table_catalog"]]..exa_upper_end..[[ as "exa_table_catalog", ]]..exa_upper_begin..[["table_schema"]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[["table_name"]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[["column_name"]]..exa_upper_end..[[ as "exa_column_name", pg.* from  
		(import from jdbc at ]]..CONNECTION_NAME..[[ statement 
			'select table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision  
				from information_schema.columns join information_schema.tables using (table_catalog, table_schema, table_name) 
				where table_type = ''BASE TABLE'' 
				AND table_schema not in (''information_schema'',''pg_catalog'')
				AND table_schema like '']]..SCHEMA_FILTER..[[''
				AND table_name like '']]..TABLE_FILTER..[[''
		') as pg order by false
)
,vv_create_schemas as(
	SELECT 'create schema if not exists "' || "exa_table_schema" || '";' as sql_text from vv_pg_columns  group by "exa_table_catalog","exa_table_schema" order by "exa_table_catalog","exa_table_schema"
)
,vv_create_tables as (
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' ||
	case 
	when "data_type" = 'ARRAY' then 'varchar(2000000)'
	when "data_type" = 'USER-DEFINED' then 'varchar(100000)' 
	when "data_type" = 'bigint' then 'BIGINT' 
	when "data_type" = 'bit' then case when "character_maximum_length"=1 then 'boolean' else 'varchar('||case when nvl("character_maximum_length",2000000) > 2000000 then 2000000 else nvl("character_maximum_length",2000000) end || ')' end
	when "data_type" = 'bit varying' then case when "character_maximum_length"=1 then 'boolean' else 'varchar('||case when nvl("character_maximum_length",2000000) > 2000000 then 2000000 else nvl("character_maximum_length",2000000) end|| ')' end 
	when "data_type" = 'boolean' then 'bool'
	when "data_type" = 'box' then 'varchar(1000)'
	when "data_type" = 'bytea' then 'varchar(2000000)'
	when "data_type" = 'character' then 'char(' || case when nvl("character_maximum_length",2000) > 2000 then 2000 else nvl("character_maximum_length",2000) end || ')' 
	when "data_type" = 'character varying' then 'varchar(' || case when nvl("character_maximum_length",2000000) > 2000000 then 2000000 else nvl("character_maximum_length",2000000) end || ')' 
	when "data_type" = 'cidr' then 'varchar(100)' 
	when "data_type" = 'circle' then 'varchar(1000)' 
	when "data_type" = 'date' then 'date'
	when "data_type" = 'double precision' then 'DOUBLE' 
	when "data_type" = 'inet' then 'varchar(100)'
	when "data_type" = 'integer' then 'INTEGER'
	when "data_type" = 'interval' then 'varchar(1000)' 
	when "data_type" = 'json' then 'varchar(2000000)'
	when "data_type" = 'jsonb' then 'varchar(2000000)'
	when "data_type" = 'line' then 'varchar(2000000)'
	when "data_type" = 'lseg' then 'varchar(50000)'
	when "data_type" = 'macaddr' then 'varchar(100)'
	when "data_type" = 'money' then 'varchar(100)' --maybe decimal instead?
	when "data_type" = 'name' then 'varchar(1000)'
	when "data_type" = 'numeric' then case when "numeric_precision" is null then 'DOUBLE' else case when "numeric_precision" > 36 and "numeric_scale" > 36 then 'decimal (36,36)' when "numeric_precision" > 36 and "numeric_scale" <= 36 then 'decimal(36, ' || "numeric_scale" || ')' else 'decimal(' || "numeric_precision" || ',' || "numeric_scale" || ')' end end
	/* alternative to keep the values with a precision/scale > 36 as a double: 
	when "data_type" = 'numeric' then case when "numeric_precision" is null or "numeric_precision" > 36 then 'DOUBLE' else 'decimal(' || "numeric_precision" || ',' || case when ("numeric_scale" > "numeric_precision") then "numeric_precision" else  case when "numeric_scale" < 0 then 0 else "numeric_scale" end end || ')' end
	*/
	when "data_type" = 'oid' then 'decimal(36)'
	when "data_type" = 'path' then 'varchar(2000000)'
	when "data_type" = 'pg_lsn' then 'varchar(2000000)'
	when "data_type" = 'point' then 'varchar(2000)'
	when "data_type" = 'polygon' then 'varchar(50000)'
	when "data_type" = 'real' then 'DOUBLE'
	when "data_type" = 'smallint' then 'SMALLINT' 
	when "data_type" = 'text' then 'varchar(2000000)'
	when "data_type" = 'time with time zone' then 'TIMESTAMP WITH LOCAL TIME ZONE' 
	when "data_type" = 'time without time zone' then 'TIMESTAMP' 
	when "data_type" = 'timestamp with time zone' then 'TIMESTAMP WITH LOCAL TIME ZONE' 
	when "data_type" = 'timestamp without time zone' then 'TIMESTAMP' 
	when "data_type" = 'tsquery' then 'varchar(2000000)'
	when "data_type" = 'tsvector' then 'varchar(2000000)'
	when "data_type" = 'txid_snapshot' then 'varchar(2000000)'
	when "data_type" = 'uuid' then 'varchar(128)'
	when "data_type" = 'xml' then 'varchar(2000000)'
	else '/*UNKNOWN_DATATYPE:' || "data_type" || '*/ varchar(2000000)' end
	order by "ordinal_position") || ');' as sql_text
	from vv_pg_columns  group by "exa_table_catalog","exa_table_schema", "exa_table_name"
	order by "exa_table_catalog","exa_table_schema","exa_table_name"
)
, vv_imports as (
	select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' || group_concat( 
	case 
	when "data_type" = 'ARRAY' then "column_name" ||'::text'
	when "data_type" = 'USER-DEFINED' then "column_name" ||'::text' 
	when "data_type" = 'bit' then "column_name" ||'::text'
	when "data_type" = 'bit varying' then "column_name" ||'::text'
	when "data_type" = 'box' then "column_name" ||'::text'
	when "data_type" = 'bytea' then "column_name" ||'::text'
	when "data_type" = 'cidr' then "column_name" ||'::text' 
	when "data_type" = 'circle' then "column_name" ||'::text' 
	when "data_type" = 'inet' then "column_name" ||'::text'
	when "data_type" = 'interval' then "column_name" ||'::text' 
	when "data_type" = 'json' then "column_name" ||'::text'
	when "data_type" = 'jsonb' then "column_name" ||'::text'
	when "data_type" = 'line' then "column_name" ||'::text'
	when "data_type" = 'lseg' then "column_name" ||'::text'
	when "data_type" = 'name' then "column_name" ||'::text'
	when "data_type" = 'macaddr' then "column_name" ||'::text'
	when "data_type" = 'money' then "column_name" ||'::text'
	when "data_type" = 'point' then "column_name" ||'::text'
	when "data_type" = 'path' then "column_name" ||'::text'
	when "data_type" = 'pg_lsn' then "column_name" ||'::text'
	when "data_type" = 'polygon' then "column_name" ||'::text'
	when "data_type" = 'timestamp with time zone' then 'case when  '||"column_name"||' > ''''9999-12-31 23:59:59.999'''' then ''''9999-12-31 23:59:59.999'''' when '||"column_name" ||' < ''''0001-01-01'''' then ''''0001-01-01'''' else '||"column_name" ||' end'
	when "data_type" = 'timestamp without time zone' then 'case when  '||"column_name"||' > ''''9999-12-31 23:59:59.999'''' then ''''9999-12-31 23:59:59.999'''' when '||"column_name" ||' < ''''0001-01-01'''' then ''''0001-01-01'''' else '||"column_name" ||' end'
	when "data_type" = 'tsquery' then "column_name" ||'::text'
	when "data_type" = 'tsvector' then "column_name" ||'::text'
	when "data_type" = 'txid_snapshot' then "column_name" ||'::text'
	when "data_type" = 'uuid' then "column_name" ||'::text'
	when "data_type" = 'xml' then "column_name" ||'::text'
	else "column_name"
	 end
	order by "ordinal_position") || ' from ' || "table_schema"|| '.' || "table_name"|| ''';' as sql_text
	from vv_pg_columns group by "exa_table_catalog","exa_table_schema","exa_table_name", "table_schema","table_name"
	order by "exa_table_catalog", "exa_table_schema","exa_table_name", "table_schema","table_name"
)
select * from vv_create_schemas
UNION ALL
select * from vv_create_tables
UNION ALL
select * from vv_imports]],{})

return(res)
/

-- Create a connection to the Postgres database
create connection postgres_db to 'jdbc:postgresql://192.168.59.103:5432/dbname' user 'username' identified by 'exasolRocks!';

-- Finally start the import process
execute script database_migration.POSTGRES_TO_EXASOL(
    'postgres_db', -- name of your database connection
    true,          -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
    '%',           -- schema filter --> '%' to load all schemas except 'information_schema' and 'pg_catalog' / '%publ%' to load all schemas like '%pub%'
    '%'            -- table filter --> '%' to load all tables 
);
