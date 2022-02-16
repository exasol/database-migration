create schema if not exists database_migration;

/* 
    This script will generate create schema, create table and create import statements 
    to load all needed data from an EXASOL database. Automatic datatype conversion is 
    applied whenever needed. Feel free to adjust it. 
*/
--/
create or replace script database_migration.EXASOL_TO_EXASOL(
  CONNECTION_NAME              -- name of the database connection inside exasol -> e.g. my_exa
  ,CONNECTION_SETTING          -- set to JDBC or EXA
  ,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
  ,SCHEMA_FILTER               -- filter for the schemas to generate and load (except EXA_SATISTICS and SYS) -> '%' to load all
  ,TABLE_FILTER                -- filter for the tables to generate and load -> '%' to load all
  ,GENERATE_VIEWS              -- flag to control inclusion of views
  ,VIEW_FILTER                 -- filter for the views to generate -> '%' to generate all
  ,PK_SETTING                  -- disable/enable to set primary key  
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
		(import from ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ statement 
			'with constr_cols as (

                                select * 
                                from "SYS"."EXA_ALL_CONSTRAINT_COLUMNS"
                                where (CONSTRAINT_TYPE IN (''PRIMARY KEY'') or CONSTRAINT_TYPE IS NULL)
                                AND constraint_schema like '']]..SCHEMA_FILTER..[[''
                                AND constraint_table like '']]..TABLE_FILTER..[[''
                                )
                                select table_schema, table_name, c.column_name, COLUMN_ORDINAL_POSITION ordinal_position, COLUMN_TYPE data_type, column_type, COLUMN_MAXSIZE character_maximum_length, COLUMN_NUM_PREC numeric_precision, COLUMN_NUM_SCALE numeric_scale, cc.constraint_name ,cc.constraint_type
                                        from EXA_ALL_COLUMNS c 
                                        join EXA_ALL_TABLES t on t.table_schema = c.column_schema and t.table_name = c.column_table
                                        left join constr_cols cc on cc.constraint_schema = c.column_schema and cc.constraint_table = c.column_table and cc.column_name = c.column_name
                                        where table_schema not in (''SYS'',''EXA_STATISTICS'')
                                        AND table_schema like '']]..SCHEMA_FILTER..[[''
                                        AND table_name like '']]..TABLE_FILTER..[[''
                                        ORDER BY c."COLUMN_ORDINAL_POSITION"
                        ') as exasql  


)
,vv_create_schemas as(
  SELECT 'create schema "' || "exa_table_schema" || '";' as sql_text from vv_exa_columns  group by "exa_table_schema" order by "exa_table_schema"
)
,vv_create_tables as (
  select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat('"' || "exa_column_name" || '" ' || data_type	||' ' order by ordinal_position) || vv_pk_constraints.PK_CON || ');' as sql_text
	from vv_exa_columns
	left join (select table_schema, table_name, constraint_name, (', CONSTRAINT "' || constraint_name || '" PRIMARY KEY (' || GROUP_CONCAT(COLUMN_NAME)) || ') ]]..PK_SETTING..[[' as PK_CON
                        from vv_exa_columns
                        where table_schema not in ('SYS','EXA_STATISTICS') AND constraint_type = 'PRIMARY KEY'
                       GROUP BY table_schema, table_name, constraint_name) as vv_pk_constraints on vv_exa_columns.table_schema = vv_pk_constraints.table_schema and vv_exa_columns.table_name = vv_pk_constraints.table_name
	WHERE (CONSTRAINT_TYPE IS NULL OR CONSTRAINT_TYPE IN ('PRIMARY KEY','FOREIGN KEY')) 
	group by "exa_table_schema", "exa_table_name", "PK_CON"
	order by "exa_table_schema","exa_table_name", "PK_CON"
)
,vv_create_foreignkey AS (
        select * from 
         (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
         'select CONCAT(''ALTER TABLE "'',CONSTRAINT_SCHEMA,''"."'',CONSTRAINT_TABLE,''" ADD CONSTRAINT "'',CONSTRAINT_NAME,''" FOREIGN KEY ('',GROUP_CONCAT(CONCAT(''"'',COLUMN_NAME,''"'')),'') REFERENCES "'',REFERENCED_SCHEMA,''"."'',REFERENCED_TABLE,''" ]]..PK_SETTING..[[;'') AS ALTER_TABLE 
         from "SYS"."EXA_ALL_CONSTRAINT_COLUMNS" 
         WHERE "EXA_ALL_CONSTRAINT_COLUMNS"."CONSTRAINT_TYPE" = ''FOREIGN KEY''
         AND "CONSTRAINT_SCHEMA" like '']]..SCHEMA_FILTER..[[''
         AND "CONSTRAINT_NAME" like '']]..TABLE_FILTER..[[''
         GROUP BY "CONSTRAINT_SCHEMA", "CONSTRAINT_TABLE","CONSTRAINT_NAME","REFERENCED_SCHEMA","REFERENCED_TABLE"') AS foreign_keys
				
)
,vv_create_distribution_key as(
select * from 
         (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
         'select CONCAT('' ALTER TABLE "'',COLUMN_SCHEMA,''"."'',COLUMN_TABLE,''" DISTRIBUTE BY '',group_concat(concat(''"'',COLUMN_NAME,''"'')),'';''  ) 
         from "SYS"."EXA_ALL_COLUMNS" 
         WHERE "EXA_ALL_COLUMNS"."COLUMN_IS_DISTRIBUTION_KEY" = TRUE
         AND COLUMN_SCHEMA like '']]..SCHEMA_FILTER..[[''
         AND COLUMN_TABLE like '']]..TABLE_FILTER..[['' 
         GROUP BY "EXA_ALL_COLUMNS"."COLUMN_SCHEMA","EXA_ALL_COLUMNS"."COLUMN_TABLE"') AS distribution_keys
)
,vv_create_partion_key as(
select alter_partion from 
         (IMPORT FROM ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ STATEMENT
         'SELECT CONCAT(COLUMN_SCHEMA,''.'',COLUMN_TABLE) as group_table_schema, CONCAT('' ALTER TABLE "'',COLUMN_SCHEMA,''"."'',COLUMN_TABLE,''" PARTITION BY '',GROUP_CONCAT(CONCAT(''"'',COLUMN_NAME,''"'') ORDER BY COLUMN_PARTITION_KEY_ORDINAL_POSITION),'';'') as alter_partion
                FROM "SYS"."EXA_ALL_COLUMNS" 
                WHERE "EXA_ALL_COLUMNS"."COLUMN_PARTITION_KEY_ORDINAL_POSITION" > 0
                AND COLUMN_SCHEMA like '']]..SCHEMA_FILTER..[[''
                AND COLUMN_TABLE like '']]..TABLE_FILTER..[['' 
                GROUP BY COLUMN_SCHEMA,COLUMN_TABLE
                ') AS partion_keys
)
, vv_imports as (
  select 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ table "' || table_schema||'"."'||table_name||'";'  as sql_text
	from vv_exa_columns group by "exa_table_schema","exa_table_name", table_schema,table_name
	order by "exa_table_schema","exa_table_name", table_schema,table_name
)
,vv_create_views as(
  select view_text || ';' as sql_text from  
		(import from ]]..CONNECTION_SETTING..[[ at ]]..CONNECTION_NAME..[[ statement 
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
select 5, cast('-- ### FOREIGN KEYS ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 6, c.* from vv_create_foreignkey c
UNION ALL
select 7, cast('-- ### PARTION BY ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 8, d.* from vv_create_partion_key d
UNION ALL
select 9, cast('-- ### DISTRIBUTION KEY ###' as varchar(2000000)) SQL_TEXT
UNION ALL
select 10, e.* from vv_create_distribution_key e
UNION ALL
select 11, cast('-- ### IMPORTS ###' as varchar(2000000)) SQL_TEXT
union all
select 12, f.* from vv_imports f
union all
select 13, cast('-- ### VIEWS - Add FORCE as needed to avoid ordering dependencies ###' as varchar(2000000)) SQL_TEXT
from dual where ]]..GENERATE_VIEWS..[[ 
union all
select 14, g.* from vv_create_views g
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
   ,'JDBC'             -- set if import from EXA or JDBC connection
   ,FALSE             -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
   ,'%TPC%'           -- schema filter --> '%' to load all schemas except 'SYS' and 'EXA_STATISTICS'/ '%pub%' to load all schemas like '%pub%'
   ,'%'               -- table filter --> '%' to load all tables
   ,'FALSE'           -- view inclusion flag --> 'TRUE' to include views
   ,'%'               -- view filter --> '%' to generate all views
   ,'DISABLE'         -- pk & fk setting --> disable/enable to create disabled/enabled primary key
);

commit;
