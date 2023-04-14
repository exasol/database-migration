create schema if not exists database_migration;

/*
     This script will generate create schema, create table and create import statements
     to load all needed data from Snowflake databases. Automatic datatype conversion is
     applied whenever needed. Feel free to adjust it.
*/
--/
create or replace script database_migration.SNOWFLAKE_TO_EXASOL(
CONNECTION_NAME 	-- name of the database connection inside exasol, e.g. snowflake_connection
,DB2SCHEMA		-- if true then Snowflake: database.schema.table => EXASOL: database.schema_table; if false then Snowflake: schema.table => EXASOL: schema.table
,DB_FILTER 		-- filter for Snowflake db, e.g. 'master', 'ma%', 'first_db, second_db', '%'
,SCHEMA_FILTER 		-- filter for the schemas to generate and load, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
,TARGET_SCHEMA          -- target schema on Exasol side, set to empty string to use values from souce database
,TABLE_FILTER 		-- filter for the tables to generate and load, e.g. 'my_table', 'my%', 'table1, table2', '%'
,IDENTIFIER_CASE_INSENSITIVE 	-- TRUE if identifiers should be put uppercase
) RETURNS TABLE
AS

exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

if string.match(DB_FILTER, '%%') then
DB_STR = 		[[LIKE '']]..DB_FILTER..[['']]
else	DB_STR			= [[in ('']]..DB_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end

output(DB_STR)

if string.match(SCHEMA_FILTER, '%%') then
SCHEMA_STR = 	[[like ('']]..SCHEMA_FILTER..[['')]]
else	SCHEMA_STR		= [[in ('']]..SCHEMA_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end

output(SCHEMA_STR)

if string.match(TABLE_FILTER, '%%') then
TABLE_STR = 		[[like ('']]..TABLE_FILTER..[['')]]
else	TABLE_STR		= [[in ('']]..TABLE_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]
end

output(TABLE_STR)

if DB2SCHEMA then

        if (TARGET_SCHEMA == null) then -- if no target schema set, use DB_NAME
                TARGET_SCHEMA = 'DB_NAME'
        else
                TARGET_SCHEMA = [[']]..TARGET_SCHEMA..[[']]
        end
	
	tbl_def = [["' || ]]..exa_upper_begin.. TARGET_SCHEMA ..exa_upper_end..[[ || '"."' || ]]..exa_upper_begin.. [[ schema_name ]] ..exa_upper_end..[[ || '_' ||  ]]..exa_upper_begin..[[  table_name ]] ..exa_upper_end..[[  || '" ]]
	tbl_group = [[DB_NAME,SCHEMA_NAME,TABLE_NAME]]
else
        if (TARGET_SCHEMA == null) then -- if no target schema set, use SCHEMA_NAME
                TARGET_SCHEMA = 'SCHEMA_NAME'
        else
                TARGET_SCHEMA = [[']]..TARGET_SCHEMA..[[']]
        end
	
	tbl_def = [["' || ]]..exa_upper_begin.. TARGET_SCHEMA ..exa_upper_end..[[ || '"."' || ]]..exa_upper_begin..[[ table_name ]] ..exa_upper_end..[[  || '"]]
	tbl_group = [[SCHEMA_NAME,TABLE_NAME]]
end

dbquery = [[select * from ( import from jdbc at ]]..CONNECTION_NAME..[[ statement 'SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME ]]..DB_STR..[[')]] 
success1, res1 = pquery(dbquery,{})
output(dbquery)
if not success1 then error('Error on getting db list from Snowflake:'..dbquery) else output('Successfully received db list from Snowflake. ') end

if (#res1) < 1 then error('No database found.') end

query_str = [[select 	'']]..res1[1][1]..[[''  as DB_NAME,
  	 s.SCHEMA_NAME  as SCHEMA_NAME,
  	 t.TABLE_NAME  as TABLE_NAME,
  	cast(c.ORDINAL_POSITION as NUMBER(36,0)) as COLUMN_ID,]]..exa_upper_begin..[[c.COLUMN_NAME]]..exa_upper_end..[[  as COLUMN_NAME,
  	cast(c.CHARACTER_MAXIMUM_LENGTH as NUMBER(36,0)) as COL_MAX_LENGTH,
  	cast(c.NUMERIC_PRECISION as NUMBER(36,0)) as PRECISION,
  	cast(c.NUMERIC_SCALE as NUMBER(36,0)) as SCALE,
  	c.IS_NULLABLE as IS_NULLABLE,
  	c.IS_IDENTITY as IS_IDENTITY,
  	c.DATA_TYPE as DATA_TYPE
 		from ]]..res1[1][1]..[[.INFORMATION_SCHEMA.SCHEMATA s
  join ]]..res1[1][1]..[[.INFORMATION_SCHEMA.TABLES t on s.SCHEMA_NAME=t.TABLE_SCHEMA
  join ]]..res1[1][1]..[[.INFORMATION_SCHEMA.COLUMNS c on (c.TABLE_NAME=t.TABLE_NAME AND c.TABLE_SCHEMA=s.SCHEMA_NAME)
  where s.SCHEMA_NAME ]]..SCHEMA_STR..[[ and t.TABLE_NAME ]]..TABLE_STR..' '

for i = 2, (#res1) do
query_str = query_str..[[
union all
]]..
		[[select 	'']]..res1[i][1]..[[''  as DB_NAME,
  	 s.SCHEMA_NAME  as SCHEMA_NAME,
  	 t.TABLE_NAME  as TABLE_NAME,
  	cast(c.ORDINAL_POSITION as NUMBER(36,0)) as COLUMN_ID,]]..exa_upper_begin..[[c.COLUMN_NAME]]..exa_upper_end..[[  as COLUMN_NAME,
  	cast(c.CHARACTER_MAXIMUM_LENGTH as NUMBER(36,0)) as COL_MAX_LENGTH,
  	cast(c.NUMERIC_PRECISION as NUMBER(36,0)) as PRECISION,
  	cast(c.NUMERIC_SCALE as NUMBER(36,0)) as SCALE,
  	c.IS_NULLABLE as IS_NULLABLE,
  	c.IS_IDENTITY as IS_IDENTITY,
  	c.DATA_TYPE as DATA_TYPE
 		from ]]..res1[i][1]..[[.INFORMATION_SCHEMA.SCHEMATA s
  join ]]..res1[i][1]..[[.INFORMATION_SCHEMA.TABLES t on s.SCHEMA_NAME=t.TABLE_SCHEMA
  join ]]..res1[i][1]..[[.INFORMATION_SCHEMA.COLUMNS c on (c.TABLE_NAME=t.TABLE_NAME AND c.TABLE_SCHEMA=s.SCHEMA_NAME)
  where s.SCHEMA_NAME ]]..SCHEMA_STR..[[ and t.TABLE_NAME ]]..TABLE_STR..' '
end

output(query_str)

success, res = pquery([[
with snowflake_base as(
	select * from(
		import from jdbc at ]]..CONNECTION_NAME..[[
		statement ']]..query_str..[['
				)
			),
	cr_schemas as ( -- if db=schema then select distinct db_name as schema_name else select distinct schema_name as schema_name
		with all_schemas as (select distinct ]]..TARGET_SCHEMA..[[ as schema_name from snowflake_base )
			select 'create schema if not exists "' || ]]..exa_upper_begin..[[ schema_name ]]..exa_upper_end..[[ ||'";' as cr_schema from all_schemas order by schema_name
	),
	cr_tables as ( -- if db=schema then db_name"."schema_name"_"table_name
		select 'create or replace table ]]..tbl_def..[[  (' || cols || '); ' || cols2 || ''
				 as tbls from (select ]]..tbl_group..[[,
 			group_concat(
 				case DATA_TYPE
 					--when 'NUMBER' then '"' || column_name || '"' ||' ' || case when PRECISION > 36 then case when SCALE > 36 then 'DECIMAL(' || 36 || ',' || 36 || ')' else 'DECIMAL(' || 36 || ',' || SCALE || ')' end else 'DECIMAL(' || PRECISION || ',' || SCALE || ')' end   --numeric
 					-- Alternative when you have big values with a precision higher than 36 inside a column numeric(38) and want to store them
 					when 'NUMBER' then '"' || column_name || '"' ||' ' || case when PRECISION > 36 then 'DECIMAL(36,'||SCALE||')' else 'DECIMAL(' || PRECISION || ',' || SCALE || ')' end --numeric
 					when 'FLOAT' then '"' || column_name || '"' ||' ' || 'FLOAT' --float
 					when 'TEXT' then '"' || column_name || '"' ||' ' ||'VARCHAR('||case when COL_MAX_LENGTH < 1 then 2000000 else case when COL_MAX_LENGTH>2000000 then 2000000 else COL_MAX_LENGTH end end || ')' --varchar
 					when 'BINARY' then '"' || column_name || '"' ||' ' ||'VARCHAR(20)' --binary
 					--when upper(DATA_TYPE) IN ('INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'BYTEINT') then '"' || column_name || '"' ||' ' ||'INTEGER'         				   --integer
 					when 'BOOLEAN' then '"' || column_name || '"' ||' ' ||'BOOLEAN' --boolean         				   --float
 					when 'GEOMETRY' then '"' || column_name || '"' ||' ' ||'GEOMETRY' --geometry
 					when 'GEOGRAPHY' then '"' || column_name || '"' ||' ' ||'GEOMETRY(4326)' --geography
 					when 'TIMESTAMP' then '"' || column_name || '"' ||' ' ||'TIMESTAMP' --timestamp 
 					when 'DATE' then '"' || column_name || '"' ||' ' ||'TIMESTAMP' -- date
 					when 'DATETIME' then '"' || column_name || '"' ||' ' ||'TIMESTAMP' --timestamp without timezone
 					when 'TIMESTAMP_LTZ' then '"' || column_name || '"' ||' ' ||'TIMESTAMP WITH LOCAL TIME ZONE' --timestamp with timezone
 					when 'TIMESTAMP_NTZ' then '"' || column_name || '"' ||' ' ||'TIMESTAMP' --timestamp without timezone
 					when 'TIME' then '"' || column_name || '"' ||' ' ||'TIMESTAMP' --timestamp without timezone
 					when 'VARIANT' then '"' || column_name || '"' ||' ' ||'VARCHAR(2000000)' -- semi-structured - placeholder value, data will not be imported
 					when 'OBJECT' then '"' || column_name || '"' ||' ' ||'VARCHAR(2000000)' -- semi-structured - placeholder value, data will not be imported
 					when 'ARRAY' then '"' || column_name || '"' ||' ' ||'VARCHAR(2000000)' -- semi-structured - placeholder value, data will not be imported
 				end
 				|| case when IS_IDENTITY='1' then ' IDENTITY' end
 				|| case when IS_NULLABLE='0' then ' NOT NULL' end

 			order by COLUMN_ID SEPARATOR ',' )
 			as cols,
                    group_concat(
                            case
                            when DATA_TYPE not in ('FLOAT', 'NUMBER', 'NUMERIC', 'DECIMAL', 'CHARACTER', 'CHAR', 'VARCHAR', 'STRING', 'TEXT', 'INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'BYTEINT', 'BOOLEAN', 'GEOMETRY', 'GEOGRAPHY', 'TIMESTAMP', 'DATETIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'DATE', 'TIME', 'VARIANT', 'OBJECT', 'ARRAY')
                            then '-- UNSUPPORTED DATATYPE IN COLUMN ' || column_name || '  Snowflake TYPE INFO: DATA_TYPE ' || DATA_TYPE || ', PRECISION ' || PRECISION || ', SCALE ' || SCALE
                            end
                    )
            as cols2
 			from snowflake_base group by ]]..tbl_group..[[ ) order by tbls
	),
	cr_import_stmts as (
		select 'import into ]]..tbl_def..[[(' || group_concat( case DATA_TYPE 
 					when 'NUMBER' then '"' || column_name || '"'
 					when 'FLOAT' then '"' || column_name || '"'
 					when 'TEXT' then  '"' || column_name || '"'
 					when 'BINARY' then '"' || column_name || '"'
 					when 'BOOLEAN'  then '"' || column_name || '"'
 					when 'GEOMETRY' then '"' || column_name || '"'
 					when 'GEOGRAPHY' then '"' || column_name || '"'
 					when 'TIMESTAMP' then '"' || column_name || '"'
 					when 'DATETIME'  then '"' || column_name || '"'
 					when 'TIMESTAMP_NTZ' then '"' || column_name || '"'
 					when 'DATE' then '"' || column_name || '"'
 					when 'TIMESTAMP_LTZ' then '"' || column_name || '"'
 					when 'TIME' then '"' || column_name || '"'
 					when 'VARIANT' then '"' || column_name || '"'
 					when 'OBJECT' then '"' || column_name || '"'
 					when 'ARRAY' then '"' || column_name || '"'
 					-- else '-- UNSUPPORTED DATATYPE IN COLUMN ' || column_name || '  Snowflake TYPE INFO: NAME ' || DATA_TYPE || ', PRECISION ' || PRECISION || ', SCALE ' || SCALE
 				end  order by column_id SEPARATOR ',
' ) || ') from jdbc at ]]..CONNECTION_NAME..[[ statement
''select
' || group_concat(case DATA_TYPE 
 					when 'NUMBER' then '"' || column_name || '"'
 					when 'FLOAT'  then '"' || column_name || '"'
 					when 'TEXT' then 'substring("'||column_name||'" ,0, case when length("'|| column_name ||'")>2000000 then 2000000 else length("'||column_name ||'") end)'
 					when 'BOOLEAN'  then '"' || column_name || '"'
 					when 'GEOMETRY' then 'ST_ASTEXT("' || column_name || '")'
 					when 'GEOGRAPHY' then 'ST_ASTEXT("' || column_name || '")'
 					when 'TIMESTAMP' then '"' || column_name || '"'
 					when 'TIME' then '"' || column_name || '"'  
 					when 'DATE'  then '"' || column_name || '"'
 					when 'DATETIME'  then '"' || column_name || '"' 
 					when 'TIMESTAMP_LTZ' then '"' || column_name || '"' 
 					when 'TIMESTAMP_NTZ' then '"' || column_name || '"' 
 					when 'BINARY' then '''''X''''' -- binary to varchar will not be imported
 					when 'VARIANT' then '''''X''''' -- variant to varchar will not be imported
 					when 'OBJECT' then '''''X''''' -- object to varchar - will not be imported
 					when 'ARRAY' then '''''X''''' -- array to varchar - will not be imported
 					-- else '-- UNSUPPORTED DATATYPE IN COLUMN ' || column_name || '  Snowflake TYPE INFO: NAME ' || DATA_TYÜE || ', PRECISION ' || PRECISION || ', SCALE ' || SCALE
 				end  order by column_id SEPARATOR ',') || 
 				'from "' || db_name || '"."' || schema_name || '"."' || table_name || '"'';'  as imp from snowflake_base group by DB_NAME,SCHEMA_NAME,TABLE_NAME order by imp
	)
select SQL_TEXT from (
select 1 as ord, '--This Snowflake instance is system-wide '|| status || '. There might be exceptions on table or column level.' as SQL_TEXT from (select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select case when ''A'' = ''a'' then ''NOT CASE SENSITIVE'' else ''CASE SENSITIVE'' end as STATUS'))
union all
select 2, cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
union all
select 3, a.* from cr_schemas a
union all
select 4, cast('-- ### TABLES ###' as varchar(2000000)) SQL_TEXT
union all
select 5, b.* from cr_tables b
where b.TBLS not like '%();%'
union all
select 6, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select 7, c.* from cr_import_stmts c
where c.IMP not like '%() from%'
) order by ord
]],{})
output(res.statement_text)
if not success then error(res.error_message) end
return(res)
/

-- Create a connection to Snowflake
CREATE OR REPLACE CONNECTION SNOWFLAKE_CONNECTION TO
  'jdbc:snowflake://<myorganization>-<myaccount>.snowflakecomputing.com/?warehouse=<my_compute_wh>&role=<my_role>&CLIENT_SESSION_KEEP_ALIVE=true'
  USER '<sfuser>' IDENTIFIED BY '<sfpwd>';

-- Finally start the import process
execute script database_migration.SNOWFLAKE_TO_EXASOL(
    'SNOWFLAKE_CONNECTION',     -- CONNECTION_NAME:      name of the database connection inside exasol -> e.g. sf_db
    true,                       -- DB2SCHEMA:            if true then Snowflake: database.schema.table => EXASOL: database.schema_table; if false then Snowflake: schema.table => EXASOL: schema.table
    '%',                        -- DB_FILTER:            filter for Snowflake db, e.g. 'master', 'ma%', 'first_db, second_db', '%'
    '%',                        -- SCHEMA_FILTER:        filter for the schemas to generate and load e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
    '',                         -- EXASOL_TARGET_SCHEMA: set to empty string to use original values
    '%',                        -- TABLE_FILTER:         filter for the tables to generate and load e.g. 'my_table', 'my%', 'table1, table2', '%'
    false                       -- IDENTIFIER_CASE_INSENSITIVE: set to TRUE if identifiers should be put uppercase
);

