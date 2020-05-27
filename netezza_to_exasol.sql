--create schema if not exists database_migration;

/* 
	This script will generate create schema, create table and create import statements 
	to load all needed data from a Netezza database. Automatic datatype conversion is
	applied whenever needed. Feel free to adjust it.
*/

--/


CREATE OR REPLACE SCRIPT database_migration.NETEZZA_TO_EXASOL(
CONNECTION_NAME                                 --Name of the database connection inside Exasol -> e.g. Netezza
,DB_FILTER 					-- filter for Netezza Database
,SCHEMA_FILTER 					-- Filter for specific Schema_Name to generate and load
,TABLE_FILTER 					-- TABLE_FILTER: Filter for specific Tables to generate and load
,IDENTIFIER_CASE_INSENSITIVE 	                -- TRUE if identifiers should be put uppercase
)RETURNS TABLE
AS

ucb=''                                           -- Beginning of uppercase
uce=''                                           -- Uppercase end
if IDENTIFIER_CASE_INSENSITIVE == true then
	ucb='upper('
	uce=')'
end


suc, res = pquery([[
with vv_netezza_columns as(
        SELECT ]]..ucb..[[schema_name]]..uce..[[ as "exa_table_schema", ]]..ucb..[[table_name]]..uce..[[ as "exa_table_name", ]]..ucb..[[column_name]]..uce..[[ as "exa_column_name", tableList.* from 
        (import from JDBC at ]]..CONNECTION_NAME..[[ statement
        'select trim(DATABASE) as database_name, trim(schema) as schema_name, table_name, column_name, trim(TYPE_NAME) as data_type, trim(ORDINAL_POSITION) as ordinal_position, trim(COLUMN_SIZE) as numeric_precision, trim(DECIMAL_DIGITS) as numeric_scale
         from ]]..DB_FILTER..[[.DEFINITION_SCHEMA._V_SYS_COLUMNS
         where schema_name like '']]..SCHEMA_FILTER..[[''
         and table_name like '']]..TABLE_FILTER..[[''
         ') as tableList)


,vv_create_schemas as(
	SELECT 'create schema if not exists "' || "exa_table_schema" || '";' as sql_text from vv_netezza_columns  group by "exa_table_schema" order by "exa_table_schema")


,vv_create_tables as(
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat(
                case
                
                -- ### Numeric Types ###
                
                when upper(data_type) = 'BYTEINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(3,0)'           -- 8-bit values in range –128 to 127,                   1 byte
                when upper(data_type) = 'SMALLINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(5,0)'          -- 16-bit values in range –32,768 to 32,767             2 byte
                when upper(data_type) = 'INTEGER' then '"' || "exa_column_name" || '" ' || 'DECIMAL(10,0)'           -- 32-bit values in range –2,147,483,648 to 2,147,483,647      4 byte
                when upper(data_type) = 'BIGINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(19,0)'            -- 64-bit values in range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 8 byte
                when upper(data_type) like ('NUMERIC%') then case
                        when numeric_precision is null then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'
                        else '"' || "exa_column_name" || '" ' || 'DECIMAL(' || case when numeric_precision > 36 then 36
                                                                                    else numeric_precision end || ',' ||
                                                                               case when numeric_scale >= 36 then (36- (numeric_precision-numeric_scale))
                                                                               else case
                                                                                        when numeric_scale is null then 0
                                                                                        when numeric_scale < 0 then 0
                                                                                        else numeric_scale end
                                                                                        end || ')' end
                                                                                        
                when upper(data_type) = 'FLOAT' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                     -- Float (p: 1-15)
                when upper(data_type) = 'REAL' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                      -- Float (6)
                when upper(data_type) = 'DOUBLE' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'    
                when upper(data_type) = 'DOUBLE PRECISION' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                    -- Float (15) 
                

                -- ### Character/String Types ###
                
                when upper(data_type) = 'CHAR' then '"' || "exa_column_name" || '" ' || 'CHAR'
                when upper(data_type) like 'CHARACTER(%)' then case
                        when numeric_precision <= 2000 then '"' || "exa_column_name" || '" ' || 'CHAR('|| ABS(numeric_precision)||')'
                        else '"' || "exa_column_name" || '" ' || 'VARCHAR('|| ABS(numeric_precision)||')' end
                when upper(data_type) like 'NATIONAL CHARACTER(%)' then case
                        when numeric_precision <= 2000 then '"' || "exa_column_name" || '" ' || 'CHAR('|| ABS(numeric_precision)||')'
                        else '"' || "exa_column_name" || '" ' || 'VARCHAR('|| ABS(numeric_precision)||')' end
                when upper(data_type) like 'CHARACTER VARYING%' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||ABS(numeric_precision)||')'
                when upper(data_type) like 'NATIONAL CHARACTER VARYING%' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||ABS(numeric_precision)||')'  
                
                
                -- ### Boolean Types ###
                when upper(data_type) = 'BOOLEAN' then '"' || "exa_column_name" || '" ' || 'BOOLEAN'
                
                -- ### Date Time Types ###
                when upper(data_type) = 'DATE' then '"' || "exa_column_name" || '" ' || 'DATE'                                                  -- DD, MM YY
                when upper(data_type) = 'TIME' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'                      --Hours, Minutes, Seconds
                when upper(data_type) = 'TIME WITH TIME ZONE' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'         -- Time + Timezone offset
                when upper(data_type) = 'TIMESTAMP' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'
                when upper(data_type) = 'INTERVAL' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'         
                
                -- ### Binary Types ###
                --when upper(data_type) = 'ST_GEOMETRY' then '"' || "exa_column_name" || '" ' || 'GEOMETRY'                                               -- 64000 bytes 
                --when upper(data_type) like 'BINARY VARYING(%)' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||(numeric_precision*2)||')'             -- 64000 bytes binary data

                end
                order by cast(ordinal_position as decimal(10,0))) ||  ');'
                
                -- ### Unknown Datatypes ###
                || group_concat (
                case when upper(data_type) not in (
                'BYTEINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION', 'CHAR',
                'BOOLEAN', 'DATE', 'TIME', 'TIME WITH TIME ZONE', 'TIMESTAMP', /*'ST_GEOMETRY',*/ 'INTERVAL')
                 AND upper(data_type) not like 'CHARACTER%' AND upper(data_type) not like 'CHARACTER VARYING%' AND upper(data_type) not like 'NATIONAL CHARACTER VARYING%' AND upper(data_type) not like 'NATIONAL CHARACTER%'
                 AND upper(data_type) not like 'NUMERIC%'
                 --AND upper(data_type) not like 'BINARY VARYING(%)'
                then ' --UNKNOWN_DATATYPE: "'|| "exa_column_name" || '" ' || upper(data_type) || ''
                end )|| ' 'as sql_text
	
	from vv_netezza_columns
        group by "exa_table_schema", "exa_table_name"
	order by "exa_table_schema","exa_table_name" )
	
, vv_imports as(

        select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' 
                || group_concat(
                case
                when upper(data_type) = 'BYTEINT' then '' || column_name || ''
                when upper(data_type) = 'SMALLINT' then '' || column_name || ''
                when upper(data_type) = 'INTEGER' then '' || column_name || ''
                when upper(data_type) = 'BIGINT' then '' || column_name || ''
                when upper(data_type) like 'NUMERIC%' then case
                        when numeric_precision >= 36 then case
                                when numeric_scale >= 36 then 'trim(trailing ''''0'''' from (round(' || column_name || ', '|| ((36 - numeric_precision) + numeric_scale) || ')))'
                                else 'trim(trailing ''''0'''' from (round(' || column_name || ', '|| (36 - numeric_precision) || ')))' end
                        else '' || column_name || '' end
                                                
                when upper(data_type) = 'FLOAT' then '' || column_name || ''
                when upper(data_type) = 'REAL' then '' || column_name || ''
                when upper(data_type) = 'DOUBLE' then '' || column_name || ''
                when upper(data_type) = 'DOUBLE PRECISION' then '' || column_name || ''
                when upper(data_type) = 'CHAR' then '' || column_name || ''
                when upper(data_type) like 'CHARACTER(%)' then case
                        when numeric_precision <=2000 then '' || column_name || ''
                        else 'CAST(' || column_name || ' as VARCHAR('||(numeric_precision)||'))' end
                when upper(data_type) like 'NATIONAL CHARACTER(%)' then case
                        when numeric_precision <=2000 then 'CAST(' || column_name || ' as CHAR('||(numeric_precision)||'))'
                        else 'CAST(' || column_name || ' as VARCHAR('||(numeric_precision)||'))' end
                when upper(data_type) like 'CHARACTER VARYING(%)' then '' || column_name || ''
                when upper(data_type) like 'NATIONAL CHARACTER VARYING(%)' then '' || column_name || ''
                when upper(data_type) = 'BOOLEAN' then '' || column_name || ''
                when upper(data_type) = 'DATE' then '' || column_name || ''
                when upper(data_type) = 'TIME' then 'CAST(' || column_name || ' as VARCHAR('||(numeric_precision)||'))'
                when upper(data_type) = 'TIME WITH TIME ZONE' then 'CAST(' || column_name || ' as VARCHAR('||(numeric_precision)||'))'
                when upper(data_type) = 'TIMESTAMP' then '' || column_name || ''
                when upper(data_type) = 'INTERVAL' then 'CAST(' || column_name || ' as VARCHAR('||(numeric_precision)||'))'
                --when upper(data_type) = 'ST_GEOMETRY' then '' || column_name || ''
                --when upper(data_type) like 'BINARY VARYING(%)' then 'CAST(bintohex(' || column_name || ') as VARCHAR('||(numeric_precision*2)||'))'
                -- 
                end
                
                order by cast(ordinal_position as decimal(10,0)))
                
                || ' from "'|| database_name || '"."'|| schema_name || '"."' || table_name|| '"'';' as sql_text
                
                
        from vv_netezza_columns group by "exa_table_schema", "exa_table_name", database_name, schema_name, table_name
        order by "exa_table_schema", "exa_table_name", database_name, schema_name , table_name
               )


select SQL_TEXT from (
select 1, a.* from vv_create_schemas a
union all 
select 2, b.* from vv_create_tables b
union all
select 3, c.* from vv_imports c
)

]])


if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return (res)
/



CREATE OR REPLACE CONNECTION netezza_connection
        TO 'jdbc:netezza://host_ip:5480/database_name'
	USER 'user'  
	IDENTIFIED BY 'exasolRocks';
	
	
EXECUTE SCRIPT database_migration.NETEZZA_TO_EXASOL(
        'netezza_connection',                   -- Connection-Name
        '%',                                    -- DB_FILTER: Filter for specific Netezza Database
        '%',                                    -- SCHEMA_FILTER: Filter for specific Schema_Name to generate and load
        '%',                                    -- TABLE_FILTER: Filter for specific Tables to generate and load
        false                                    -- IDENTIFIER_CASE_INSENSITIVE: Case Sensitivity handling for identifiers -> FALSE: handle them case sensitive / TRUE: handle them case insensitiv --> recommended: true
);





