create schema if not exists database_migration;

/* 
     This script will generate create schema, create table and create import statements 
     to load all needed data from a teradata database. Automatic datatype conversion is 
     applied whenever needed. Feel free to adjust it. 
*/
--/

create or replace script database_migration.TERADATA_TO_EXASOL(
        CONNECTION_NAME              --name of the database connection inside exasol -> e.g. teradata_db
        ,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
        ,SCHEMA_FILTER               --filter for the schemas to generate and load (except DBC)  -> '%' to load all
        ,TABLE_FILTER                --filter for the tables to generate and load -> '%' to load all
        ) RETURNS TABLE
AS

exa_upper_begin=''
exa_upper_end=''

if IDENTIFIER_CASE_INSENSITIVE == true then
        exa_upper_begin='upper('
	exa_upper_end=')'
end

res = query([[
with vv_columns as (
	select         ]]..exa_upper_begin..[["table_schema"]]..exa_upper_end..[[ as "exa_table_schema", 
	               ]]..exa_upper_begin..[["table_name"]]..exa_upper_end..[[ as "exa_table_name", 
	               ]]..exa_upper_begin..[["column_name"]]..exa_upper_end..[[ as "exa_column_name"
	               , tableList.* 
        from            (import from jdbc at ]]..CONNECTION_NAME..[[ 
        statement      'select trim(c.DatabaseName) as  table_schema, 
                                trim(c.TableName) as    table_name, 
                                ColumnName as           column_name,
                                ColumnId as             ordinal_position, 
                                trim(ColumnType) as     data_type, 
                                columnLength as         character_maximum_length,
                                DecimalTotalDigits as   numeric_precision, 
                                DecimalFractionalDigits as numeric_scale,
                                DecimalFractionalDigits as datetime_precision  
                        from    DBC.ColumnsV c
                        join    DBC.TablesV t on 
                                c.databaseName=t.DatabaseName AND 
                                c.TableName=t.TableName AND 
                                TableKind=''T''
                        where   table_schema not in (''DBC'') AND
                                table_schema like '']]..SCHEMA_FILTER..[['' AND
                                table_name like '']]..TABLE_FILTER..[[''
		      ') as tableList order by false),

vv_create_schemas as(
	SELECT 'create schema "' || "exa_table_schema" || '";' as sql_text 
	from vv_columns  
	group by "exa_table_schema" 
	order by "exa_table_schema"
)
,vv_create_tables as (
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' ||
	case 
	when "data_type" = 'DA' then 'DATE' 
	when "data_type" = 'BV' then 'varchar(' || 
	       case 
	       when nvl("character_maximum_length",2000000) > 2000000 then 
	               2000000 
	               else 
	                       nvl("character_maximum_length",2000000) 
	               end || ')' 
        when "data_type" = 'D'  then 
                case 
                when "numeric_precision" is null or "numeric_precision" > 36 then 
                        'DOUBLE' 
                        else 'decimal(' || "numeric_precision" || ',' || 
                        case when ("numeric_scale" > "numeric_precision") then 
                                "numeric_precision" 
                                else  
                                        case 
                                        when "numeric_scale" < 0 then 
                                                0 
                                                else 
                                                        "numeric_scale" 
                                        end 
                        end || ')' 
                end 
	when "data_type" = 'TS' then 'TIMESTAMP' 
	when "data_type" = 'CF' then 'char(' || 
	       case 
	       when nvl("character_maximum_length",2000) > 2000 then 
	       2000 
	       else 
	               nvl("character_maximum_length",2000) 
               end || ')' 
	when "data_type" = 'I1' then 'DECIMAL(9)'
	when "data_type" = 'I2' then 'DECIMAL(9)'
	when "data_type" = 'I8' then 'DECIMAL(19)' --maybe 18 but can result in errors while importing
	when "data_type" = 'AT' then 'TIMESTAMP' 
	when "data_type" = 'F'  then 'DOUBLE' 
	when "data_type" = 'CV' then 
	       'varchar(' || case when nvl("character_maximum_length",2000000) > 2000000 then 
	       2000000 
	       else 
	               nvl("character_maximum_length",2000000) 
               end || ')' 
	when "data_type" = 'I'  then 'DECIMAL(10)' --maybe 9 but can result in errors while importing
	when "data_type" = 'N'  then 
	       case when "numeric_precision" is null or "numeric_precision" > 36 then 
	       'DOUBLE' 
	       else 'decimal(' || "numeric_precision" || ',' || 
	               case when ("numeric_scale" > "numeric_precision") 
	               then "numeric_precision" else  
	                       case when "numeric_scale" < 0 then 
	                       0
	                       else "numeric_scale" 
	                       end 
                       end || ')' 
               end 
        when "data_type" = 'YR'  then  --INTERVAL YEAR 
          'INTERVAL YEAR (' || "numeric_precision" ||  ') TO MONTH'
        when "data_type" = 'YM'  then  --INTERVAL YEAR TO MONTH
          'INTERVAL YEAR (' || "numeric_precision" ||  ') TO MONTH' 
        when "data_type" = 'MO'  then  --INTERVAL MONTH 
          'INTERVAL YEAR (4) TO MONTH'  
        when "data_type" = 'DY'  then  --INTERVAL DAY 
          'INTERVAL DAY (4) TO SECOND' 
        when "data_type" = 'DH'  then  --INTERVAL DAY TO HOUR
          'INTERVAL DAY (4) TO SECOND'    
        when "data_type" = 'DM'  then  --INTERVAL DAY TO MINUTE
          'INTERVAL DAY (4) TO SECOND'  
        when "data_type" = 'DS'  then  --INTERVAL DAY TO SECOND  
          'INTERVAL DAY (4) TO SECOND (' ||    "numeric_scale" || ')'
        when "data_type" = 'HR'  then  --INTERVAL HOUR 
          'INTERVAL DAY (4) TO SECOND' 
        when "data_type" = 'HM'  then  --INTERVAL HOUR TO MINUTE  
          'INTERVAL DAY (4) TO SECOND '  
        when "data_type" = 'HS'  then  --INTERVAL HOUR TO SECOND  
          'INTERVAL DAY (4) TO SECOND (' ||    "numeric_scale" || ')' 
        when "data_type" = 'MI'  then  --INTERVAL MINUTE
          'INTERVAL DAY (4) TO SECOND'  
        when "data_type" = 'MS'  then  --INTERVAL MINUTE TO SECOND  
          'INTERVAL DAY (4) TO SECOND (' ||    "numeric_scale" || ')'   
        when "data_type" = 'SC'  then  --INTERVAL SECOND
           'INTERVAL DAY (4) TO SECOND (' ||    "numeric_scale" || ')'      
	else '/*UNKNOWN_DATATYPE:' || "data_type" || '*/ varchar(2000000)' 
	end
	
	order by       "ordinal_position") || ');' as sql_text
	from           vv_columns  
	group by       "exa_table_schema", "exa_table_name"
	order by       "exa_table_schema","exa_table_name"
)
, vv_imports as (
	select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' || group_concat( 
	case 
	when "data_type" = 'DA' then "column_name"
	when "data_type" = 'BV' then "column_name"
	when "data_type" = 'D'  then "column_name"
	when "data_type" = 'TS' then "column_name"
	when "data_type" = 'CF' then "column_name"
	when "data_type" = 'I1' then "column_name"
	when "data_type" = 'I2' then "column_name"
	when "data_type" = 'I8' then "column_name"
	when "data_type" = 'AT' then "column_name"
	when "data_type" = 'F'  then "column_name"
	when "data_type" = 'CV' then "column_name"
	when "data_type" = 'I'  then "column_name"
	when "data_type" = 'N'  then "column_name"
	when "data_type" = 'YR'  then 'cast(cast('|| "column_name" || ' AS INTERVAL YEAR  TO MONTH ) AS VARCHAR(50))'  --Interval Year
	when "data_type" = 'YM'  then 'cast('|| "column_name" || ' AS VARCHAR(50) )'  --Interval Year to Month
	when "data_type" = 'MO'  then 'cast(cast('|| "column_name" || ' AS INTERVAL YEAR  TO MONTH ) AS VARCHAR(50))'  --Interval Month
	when "data_type" = 'DY'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) ' --Interval Day
	when "data_type" = 'DH'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Day to hour
	when "data_type" = 'DM'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Day to minute
	when "data_type" = 'DS'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval day to second
	when "data_type" = 'HR'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Hour
	when "data_type" = 'HM'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Day to minute
	when "data_type" = 'HS'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval day to second
	when "data_type" = 'MI'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) ' --Interval Minute
	when "data_type" = 'MS'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval minute to second
	when "data_type" = 'SC'  then 'cast(cast('|| "column_name" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval Second
	else "column_name"
	end
	order by "ordinal_position") || ' from ' || "table_schema"|| '.' || "table_name"|| ''';' as sql_text
	from vv_columns 
	group by "exa_table_schema","exa_table_name", "table_schema","table_name"
	order by "exa_table_schema","exa_table_name", "table_schema","table_name"
)
select * from vv_create_schemas
UNION ALL
select * from vv_create_tables
UNION ALL
select * from vv_imports]],{})

return(res)
/

-- !!! Important: Please upload the Teradata JDBC-Driver via EXAOperation (Webinterface) !!!
-- !!! you can see a similar example for Oracle here: https://www.exasol.com/support/browse/SOL-179 !!!

-- Create a connection to the Teradata database
create or replace connection teradata_db to 'jdbc:teradata://192.168.56.1/CHARSET=UTF8' user 'dbc' identified by 'dbc';
-- Depending on your Teradata installation, CHARSET=UTF16 could be the better choice - otherwise you get errors like this one:
-- [42636] ETL-3003: [Column=5 Row=0] [String data right truncation. String length exceeds limit of 2 characters] (Session: 1611884537138472475)
-- In that case, configure your connection like this:
-- create connection teradata_db to 'jdbc:teradata://some.teradata.host.internal/CHARSET=UTF16' user 'db_username' identified by 'exasolRocks!';

-- Finally start the import process
execute script database_migration.TERADATA_TO_EXASOL(
    'TERADATA_DB'     -- name of your database connection
    ,true             -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
    ,'MIGRATION'     -- schema filter --> '%' to load all schemas except 'DBC' / '%pub%' to load all schemas like '%pub%'
    ,'%'              -- table filter --> '%' to load all tables
);

