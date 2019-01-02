create schema if not exists database_migration;

/* 
     This script will generate create schema, create table and create import statements 
     to load all needed data from a DB2 database. Automatic datatype conversion is 
     applied whenever needed. Feel free to adjust it. 
*/
--/
create or replace script database_migration.DB2_TO_EXASOL (
  CONNECTION_NAME              -- name of the database connection inside exasol -> e.g. postgres_db
  ,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
  ,SCHEMA_FILTER               -- filter for the schemas to generate and load (except information_schema and pg_catalog) -> '%' to load all
  ,TABLE_FILTER                -- filter for the tables to generate and load -> '%' to load all
)
RETURNS TABLE
AS
exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
  exa_upper_begin='upper('
  exa_upper_end=')'
end
summary = {}

res = query([[
with vv_db2_columns as (
  select ]]..exa_upper_begin..[[table_catalog]]..exa_upper_end..[[ as "exa_table_catalog", ]]..exa_upper_begin..[[table_schema]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[[table_name]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", db2.* from  
    (import from jdbc at ]]..CONNECTION_NAME..[[ statement 
      'select t.table_catalog, rtrim(t.table_schema) table_schema, t.table_name, column_name, ordinal_position, data_type, character_maximum_length, numeric_precision, numeric_scale, datetime_precision  
        from sysibm.columns c join sysibm.tables t on t.table_catalog = c.table_catalog and t.table_schema = c.table_schema and t.table_name = c.table_name
        where t.table_type = ''BASE TABLE'' 
        AND t.table_schema not in (''SYSCAT'',''SYSIBM'', ''SYSIBMADM'', ''SYSPUBLIC'', ''SYSSTAT'', ''SYSTOOLS'')
        AND t.table_schema like '']]..SCHEMA_FILTER..[[''
        AND t.table_name like '']]..TABLE_FILTER..[[''
    ') as db2 order by false
)
,vv_create_schemas as(
  SELECT 'create schema if not exists "' || "exa_table_schema" || '";' as sql_text from vv_db2_columns  group by "exa_table_catalog","exa_table_schema" order by "exa_table_catalog","exa_table_schema"
),vv_create_tables as (
  select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat(
    case 
    -- ### numeric types ###
    when upper(data_type) in ('BIGINT', 'DECIMAL', 'INTEGER', 'SMALLINT')  then '"' || "exa_column_name" || '" ' || 'decimal(' || numeric_precision || ',' || coalesce(numeric_scale,0)  || ')'  
    when upper(data_type) in ('DECFLOAT', 'DOUBLE PRECISION', 'REAL')  then '"' || "exa_column_name" || '" ' || 'DOUBLE'  

    -- ### date and time types ###
    when upper(data_type) = 'DATE' then '"' || "exa_column_name" || '" ' || 'DATE'    
    when upper(data_type) = 'TIMESTAMP' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'
    when upper(data_type) = 'TIME' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'
    -- ### string types ###
    when upper(data_type) in ('CHARACTER', 'GRAPHIC'  )  then '"' || "exa_column_name" || '" ' || 'char(' || character_maximum_length  || ')'  
    when upper(data_type) in ('CHARACTER VARYING', 'GRAPHIC VARYING') then '"' || "exa_column_name" || '" ' || 'varchar(' || character_maximum_length  || ')'  
    when upper(data_type) in ('CHARACTER LARGE OBJECT', 'DOUBLE-BYTE CHARACTER LARGE OBJECT') then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
    
    -- ### fallback for unknown types ###
    --else '/*UNKNOWN_DATATYPE:' || data_type || '*/ varchar(2000000)'
    end
    order by ordinal_position) || ');'
  -- ### unknown types ###
  || group_concat (
         case 
         when upper(data_type) not in ('BIGINT', 'DECIMAL', 'INTEGER', 'SMALLINT', 'DECFLOAT', 'DOUBLE PRECISION', 'REAL', 'DATE','TIME', 'TIMESTAMP', 'CHARACTER', 'GRAPHIC', 'CHARACTER VARYING', 'GRAPHIC VARYING', 'CHARACTER LARGE OBJECT', 'DOUBLE-BYTE CHARACTER LARGE OBJECT')
         then '--UNKNOWN_DATATYPE: "'|| "exa_column_name" || '" ' || upper(data_type) || ''
         end
  )|| ' 'as sql_text
  from vv_db2_columns  group by "exa_table_catalog","exa_table_schema", "exa_table_name"
  order by "exa_table_catalog","exa_table_schema","exa_table_name"
), vv_imports as (
  select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' 
           || group_concat(
                         case 
                         when upper(data_type) = 'BINARY' then 'cast('||column_name||' as char('||character_maximum_length||'))'                           
                         when upper(data_type) = 'DECFLOAT' then 'cast('||column_name||' as double)'                           
                         when upper(data_type) in ('CHARACTER LARGE OBJECT', 'DOUBLE-BYTE CHARACTER LARGE OBJECT') then 'cast('||column_name||' as varchar(32500))'                           
                         when upper(data_type) in ('BIGINT', 'DECIMAL', 'INTEGER', 'SMALLINT', 'DOUBLE PRECISION', 'REAL', 'DATE', 'TIME', 'TIMESTAMP', 'CHARACTER', 'GRAPHIC', 'CHARACTER VARYING', 'GRAPHIC VARYING')
                         then column_name 
                         end order by ordinal_position) 
           || ' from ' || table_schema|| '.' || table_name|| ''';' as sql_text
  from vv_db2_columns group by "exa_table_catalog","exa_table_schema","exa_table_name", table_schema,table_name
  order by "exa_table_catalog", "exa_table_schema","exa_table_name", table_schema,table_name
), base as
(
select 1 id, cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
union all 
select 2, s.* from vv_create_schemas s
union all
select 3, cast('-- ### TABLES ###' as varchar(2000000)) SQL_TEXT
union all
select 4, t.* from vv_create_tables t
where t.SQL_TEXT not like '%();%'
union all
select 5, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select 6, i.* from vv_imports i
where i.SQL_TEXT not like '%select  from%'
)
select SQL_TEXT from base order by id

]],{})
return res
/

-- !!! Important: Please upload the DB2 JDBC-Driver via EXAOperation (Webinterface) !!!
-- !!! you can find a small howto here: https://www.exasol.com/support/browse/SOL-213 !!!

-- Create a connection to the DB2 database
create or replace CONNECTION db2_connection 
	TO 'jdbc:db2://<<DB2_host>>:50000/<<database_name>>'
	USER 'user'
	IDENTIFIED BY 'exasolRocks!';


execute script database_migration.DB2_TO_EXASOL(
   'DB2_CONNECTION',      -- name of your database connection
   true,       -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
   '%TEST%',   -- schema filter --> '%' to load all schemas except 'SYSCAT','SYSIBM', 'SYSIBMADM', 'SYSPUBLIC', 'SYSSTAT', 'SYSTOOLS' / '%publ%' to load all schemas like '%publ%'
   '%'         -- table filter --> '%' to load all tables 
);