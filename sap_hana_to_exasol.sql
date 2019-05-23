--create schema if not exists database_migration;
/* 
	This script will generate create schema, create table and create import statements 
	to load all needed data from a SAP Hana database. Automatic datatype conversion is
	applied whenever needed. Feel free to adjust it.
*/

--/

create or replace script database_migration.HANA_TO_EXASOL(
CONNECTION_NAME         --Name of the database connection inside Exasol -> e.g. SAP Hana
, IDENTIFIER_CASE_INSENSITIVE 	-- true if identifiers should be stored case-insensitiv (will be stored upper_case)
, SCHEMA_FILTER         -- Filter for the Schemas to generate columns and load data
, TABLE_FILTER          -- Filter for the Tables to generate columns and load data
)RETURNS TABLE
AS

ucb=''                                          -- Beginning of uppercase
uce=''                                           -- Uppercase end
if IDENTIFIER_CASE_INSENSITIVE == true then
	ucb='upper('
	uce=')'
end


suc, res = pquery([[
 with vv_hana_columns as(
            SELECT ]]..ucb..[[schema_name]]..uce..[[ as "exa_table_schema", ]]..ucb..[[table_name]]..uce..[[ as "exa_table_name", ]]..ucb..[[column_name]]..uce..[[ as "exa_column_name", tableList.* from 
            (import from JDBC at ]]..CONNECTION_NAME..[[ statement
             'select schema_name, table_name, column_name, trim(DATA_TYPE_NAME) as data_type, trim(POSITION) as ordinal_position, trim(LENGTH) as numeric_precision, trim(SCALE) as numeric_scale
              from sys.columns
              where schema_name like '']]..SCHEMA_FILTER..[[''
              and table_name like '']]..TABLE_FILTER..[[''
              ') as tableList)
     
                     
,vv_create_schemas as(
	SELECT 'create schema if not exists "' || "exa_table_schema" || '";' as sql_text from vv_hana_columns  group by "exa_table_schema" order by "exa_table_schema")
	

,vv_create_tables as(
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat(
		case
		
		-- ### Numeric Types ###
		
		when upper(data_type) = 'TINYINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(3,0)'                                     --8 bit unsigned Integer
		when upper(data_type) = 'SMALLINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(5,0)'                                    --16 bit signed Integer
		when upper(data_type) = 'INTEGER' then '"' || "exa_column_name" || '" ' || 'DECIMAL(10,0)'                                    --32 bit signed Interger
		when upper(data_type) = 'BIGINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(19,0)'                                     --64 bit signed Interger
                when upper(data_type) = 'REAL' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                                    --Real precision, 32 bit Floating Point Number
                when upper(data_type) = 'DOUBLE' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                                  --64 bit Floating Point Number
                when upper(data_type) = 'DECIMAL' then
                case when numeric_precision is null then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'
                else '"' || "exa_column_name" || '" ' || 'DECIMAL(' || case when numeric_precision > 36 then 36
                                                                            else numeric_precision end || ',' ||
                                                                       case when numeric_scale >= 36 then (36- (numeric_precision-numeric_scale))
                                                                            else case
                                                                                when numeric_scale is null then 0
                                                                                when numeric_scale < 0 then 0
                                                                                else numeric_scale end
                                                                                end || ')' end
                
                when upper(data_type) = 'SMALLDECIMAL' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                              -- Small Decimal Value
                when upper(data_type) = 'FLOAT' then '"' || "exa_column_name" || '" ' || 'DOUBLE PRECISION'                                     -- 32-bit or 64-bit real number
                
                
                
                -- ### Date Time Types ###
                when upper(data_type) = 'DATE' then '"' || "exa_column_name" || '" ' || 'DATE'                                                  -- Year-Month-Day
                when upper(data_type) = 'TIME' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'                      -- Hours-Minutes-Seconds
                when upper(data_type) = 'SECONDDATE' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'                -- Y-M-D-H-M-S
                when upper(data_type) = 'TIMESTAMP' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'                                        -- Date and Time
                
                
                -- ### Boolean Types ###
                when upper(data_type) = 'BOOLEAN' then '"' || "exa_column_name" || '" ' || 'BOOLEAN'
                
                
                -- ### Character/String Types ###
                when upper(data_type) = 'CHAR' then '"' || "exa_column_name" || '" ' || 'CHAR('||numeric_precision||')'
                when upper(data_type) = 'NCHAR' then '"' || "exa_column_name" || '" ' || 'CHAR('||numeric_precision||')'
                when upper(data_type) = 'VARCHAR' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'                   -- 8000 Characters 7bit-ASCII 
                when upper(data_type) = 'NVARCHAR' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'                  -- 4000 Characters UNICODE
                when upper(data_type) = 'ALPHANUM' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'                  -- Alphanumeric Characters
                when upper(data_type) = 'SHORTTEXT' then '"' || "exa_column_name" || '" ' || 'VARCHAR('||numeric_precision||')'                 -- Strings with Search-Function
                
                
                -- ### Binary Values ###
                when upper(data_type) = 'BINARY' then case                                                                                      -- Binary Value
                        when numeric_precision < 1000 then '"' || "exa_column_name" || '" ' || 'CHAR('||(numeric_precision*2)||')'
                        else '"' || "exa_column_name" || '" ' || 'CHAR('||(2000)||')' end
                        
                when upper(data_type) = 'VARBINARY' then case                                                                                   -- Variable Binary Value
                        when numeric_precision < 1000000 then '"' || "exa_column_name" || '" ' || 'VARCHAR('||(numeric_precision*2)||')'
                        else '"' || "exa_column_name" || '" ' || 'VARCHAR('||(2000000)||')' end
                
                
                -- ### Large Object Types ###
                
                when upper(data_type) = 'NCLOB' then '"' || "exa_column_name" || '" ' || 'VARCHAR(2000000)'                                     -- Large Unicode
                --when upper(data_type) = 'BLOB' then '"' || "exa_column_name" || '" ' ||  'VARCHAR(2000000)'                                   -- Large amount of Binary
                when upper(data_type) = 'CLOB' then '"' || "exa_column_name" || '" ' ||  'VARCHAR(2000000)'                                     -- Large amount of ASCII
                when upper(data_type) = 'TEXT' then '"' || "exa_column_name" || '" ' ||  'VARCHAR(2000000)'                                     -- Enables Text Search
                when upper(data_type) = 'BINTEXT' then '"' || "exa_column_name" || '" ' || 'VARCHAR(2000000)'                                   -- Supports Text Search- Ins. Bin 
                
                
                -- ### Spatial Types ###
                when upper(data_type) = 'ST_GEOMETRY' then '"' || "exa_column_name" || '" ' || 'GEOMETRY'                                       -- Geometry Point, Supports: ST_CircularString, ST_GeometryCollection, ST_LineString, ST_MultiLineString, ST_MultiPoint, ST_MultiPolygon ST_Polygon
                when upper(data_type) = 'ST_POINT' then '"' || "exa_column_name" || '" ' || 'GEOMETRY'                                          -- Large Unicode ST_Point
                      

		end
		order by ordinal_position) ||  ');' 
		
		-- ### Unknown Datatypes ###
		
	       || group_concat (
	       case when upper(data_type) not in (
	       'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'REAL', 'DOUBLE', 'DECIMAL', 'SMALLDECIMAL', 'FLOAT', 'DATE', 'TIME', 'SECONDDATE', 'TIMESTAMP', 'BOOLEAN', 'CHAR', 'NCHAR',
	        'VARCHAR', 'NVARCHAR', 'ALPHANUM', 'SHORTTEXT', 'BINARY', 'VARBINARY', /*'BLOB',*/ 'NCLOB', 'CLOB', 'TEXT', 'BINTEXT', 'ST_GEOMETRY', 'ST_POINT')
	       then '--UNKNOWN_DATATYPE: "'|| "exa_column_name" || '" ' || upper(data_type) || ''
	       
	       end )|| ' 'as sql_text

	from vv_hana_columns
        group by "exa_table_schema", "exa_table_name"
	order by "exa_table_schema","exa_table_name" )
	

, vv_imports as(

                select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' 
                || group_concat(
                case
                when upper(data_type) = 'TINYINT' then '' || column_name || '' 
                when upper(data_type) = 'SMALLINT' then '' || column_name || ''
                when upper(data_type) = 'INTEGER' then '' || column_name || ''
                when upper(data_type) = 'BIGINT' then '' || column_name || ''
                when upper(data_type) = 'REAL' then '' || column_name || ''
                when upper(data_type) = 'DOUBLE' then '' || column_name || ''
                when upper(data_type) = 'DECIMAL' then '' || column_name || ''
                when upper(data_type) = 'SMALLDECIMAL' then '' || column_name || ''
                when upper(data_type) = 'FLOAT' then '' || column_name || ''
                when upper(data_type) = 'DATE' then '' || column_name || ''
                when upper(data_type) = 'TIME' then 'TO_VARCHAR(' || column_name || ')'
                when upper(data_type) = 'SECONDDATE' then '' || column_name || ''
                when upper(data_type) = 'TIMESTAMP' then '' || column_name || ''
                when upper(data_type) = 'BOOLEAN' then '' || column_name || ''
                when upper(data_type) = 'CHAR' then '' || column_name || ''
                when upper(data_type) = 'NCHAR' then '' || column_name || ''
                when upper(data_type) = 'VARCHAR' then '' || column_name || ''
                when upper(data_type) = 'NVARCHAR' then '' || column_name || ''
                when upper(data_type) = 'ALPHANUM' then '' || column_name || ''
                when upper(data_type) = 'SHORTTEXT' then '' || column_name || ''
                when upper(data_type) = 'BINARY' then case
                     when numeric_precision < 1000 then 'CAST(' || column_name || ' as CHAR('||(numeric_precision*2)||'))'
                     else 'CAST(' || column_name || ' as CHAR('||(2000)||'))' end
                
                when upper(data_type) = 'VARBINARY' then 'CAST(' || column_name || ' as VARCHAR('||(numeric_precision*2)||'))'
                when upper(data_type) = 'NCLOB' then 'CAST(' || column_name || ' as VARCHAR(2000000))'
                --when upper(data_type) = 'BLOB' then 'BINTOSTR(TO_VARBINARY(' || column_name || '))'
                when upper(data_type) = 'CLOB' then '' || column_name || ''
                when upper(data_type) = 'TEXT' then 'TO_VARCHAR(' || column_name || ')'
                when upper(data_type) = 'BINTEXT' then 'BINTOSTR(TO_VARBINARY(' || column_name || '))'
                when upper(data_type) = 'ST_GEOMETRY' then '' || column_name || '.ST_AsText()'
                when upper(data_type) = 'ST_POINT' then '' || column_name || '.ST_AsText()'
                end 
                
                order by ordinal_position)
                || ' from "' || schema_name|| '"."' || table_name|| '"'';' as sql_text
	       
	       from vv_hana_columns group by "exa_table_schema","exa_table_name", schema_name, table_name
	       order by "exa_table_schema","exa_table_name", schema_name, table_name

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

	
CREATE OR REPLACE CONNECTION hana_connection
        TO 'jdbc:sap://10.0.2.4:39015'
	USER 'user'  
	IDENTIFIED BY 'exasolRocks';
	

EXECUTE SCRIPT database_migration.HANA_TO_EXASOL(
 'hana_connection'      --Connection-Name
, false                 -- Case Sensitivity handling for identifiers -> FALSE: handle them case sensitive / TRUE: handle them case insensitiv --> recommended: true
,'%'                    --Schema-Filter: '%' to load all schemas
,'%'                    --Table-Filter: '%' to load all tables

);
