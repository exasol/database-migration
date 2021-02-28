create schema if not exists database_migration;

/* 
    This script will generate create schema, create table and create import statements 
    to load all needed data from an EXASOL database. Automatic datatype conversion is 
    applied whenever needed. Feel free to adjust it. 
*/
--/
create or replace script database_migration.EXASOL_TO_EXASOL(
  CONNECTION_NAME              -- name of the database connection inside exasol -> e.g. my_exa
  ,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
  ,SCHEMA_FILTER               -- filter for the schemas to generate and load (except EXA_SATISTICS and SYS) -> '%' to load all
  ,TABLE_FILTER                -- filter for the tables to generate and load -> '%' to load all
  ,GENERATE_VIEWS              -- flag to control inclusion of views
  ,VIEW_FILTER                 -- filter for the views to generate -> '%' to generate all
) RETURNS TABLE
AS
exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end
suc, res = pquery([[
with vv_exa_columns as (
  select ]]..exa_upper_begin..[[table_schema]]..exa_upper_end..[[ as "exa_table_schema", ]]..exa_upper_begin..[[table_name]]..exa_upper_end..[[ as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", exasql.* from  
		(import from exa at ]]..CONNECTION_NAME..[[ statement 
			'select table_schema, table_name, column_name, COLUMN_ORDINAL_POSITION ordinal_position, COLUMN_TYPE data_type, column_type, COLUMN_MAXSIZE character_maximum_length, COLUMN_NUM_PREC numeric_precision, COLUMN_NUM_SCALE numeric_scale
				from EXA_ALL_COLUMNS c join EXA_ALL_TABLES t on t.table_schema = c.column_schema and t.table_name = c.column_table
				where table_schema not in (''SYS'',''EXA_STATISTICS'')
				AND table_schema like '']]..SCHEMA_FILTER..[[''
				AND table_name like '']]..TABLE_FILTER..[[''
		') as exasql 


)
,vv_create_schemas as(
  SELECT 'create schema "' || "exa_table_schema" || '";' as sql_text from vv_exa_columns  group by "exa_table_schema" order by "exa_table_schema"
)
,vv_create_tables as (
  select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' || data_type	
	                                                                                                               order by ordinal_position) || ');' as sql_text
	from vv_exa_columns  group by "exa_table_schema", "exa_table_name"
	order by "exa_table_schema","exa_table_name"
)
, vv_imports as (
  select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from exa at ]]..CONNECTION_NAME..[[ table "' || table_schema||'"."'||table_name||'";'  as sql_text
	from vv_exa_columns group by "exa_table_schema","exa_table_name", table_schema,table_name
	order by "exa_table_schema","exa_table_name", table_schema,table_name
)
,vv_create_views as(
  select view_text || ';' as sql_text from  
		(import from exa at ]]..CONNECTION_NAME..[[ statement 
			'select view_text from EXA_ALL_VIEWS
				where view_schema like '']]..SCHEMA_FILTER..[[''
				and ]]..GENERATE_VIEWS..[[
				and view_name like '']]..VIEW_FILTER..[[''
				order by view_schema, view_name
		') as exasql 
)
select SQL_TEXT from (
select 1 as ord, cast('-- ### SCHEMAS ###' as varchar(2000000)) SQL_TEXT
union all 
select 2, a.* from vv_create_schemas a
UNION ALL
select 3, cast('-- ### TABLES ###' as varchar(2000000)) SQL_TEXT
union all
select 4, b.* from vv_create_tables b
UNION ALL
select 5, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select 6, c.* from vv_imports c
union all
select 7, cast('-- ### VIEWS - Add FORCE as needed to avoid ordering dependencies ###' as varchar(2000000)) SQL_TEXT
from dual where ]]..GENERATE_VIEWS..[[ 
union all
select 8, d.* from vv_create_views d
) order by ord
]],{})

if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return(res)
/

-- Create a connection to the your other Exasol database
create connection SECOND_EXASOL_DB to '192.168.6.11..14:8563' user 'username' identified by 'exasolRocks!';


execute script database_migration.EXASOL_TO_EXASOL(
   'SECOND_EXASOL_DB' -- name of your database connection
   ,TRUE              -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
   ,'MY_SCHEMA'       -- schema filter --> '%' to load all schemas except 'SYS' and 'EXA_STATISTICS'/ '%pub%' to load all schemas like '%pub%'
   ,'%'               -- table filter --> '%' to load all tables
   ,'FALSE'           -- view inclusion flag --> 'TRUE' to include views
   ,'%'               -- view filter --> '%' to generate all views
);

commit;
