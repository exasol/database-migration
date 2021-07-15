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
        ,CHECK_MIGRATION			 --true if checking tables and summary should be created
        ) RETURNS TABLE
AS

exa_upper_begin=''
exa_upper_end=''


if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

check_control = 0
if CHECK_MIGRATION then 
	check_control = 1
end

res = query([[
with vv_columns as (
	select         ]]..exa_upper_begin..[["table_schema"]]..exa_upper_end..[[ as "exa_table_schema", 
	               ]]..exa_upper_begin..[["table_name"]]..exa_upper_end..[[ as "exa_table_name", 
	               ]]..exa_upper_begin..[["column_name"]]..exa_upper_end..[[ as "exa_column_name"
	               , '"' || "column_name" || '"' as "column_name_delimited"
	               , tableList.* 
        from            (import from jdbc at ]]..CONNECTION_NAME..[[ 
        statement      'select trim(c.DatabaseName) as  table_schema, 
                                trim(c.TableName) as    table_name, 
                                ColumnName as           column_name,
                                ColumnId as             ordinal_position, 
                                trim(ColumnType) as     data_type, 
                                cast(case when ColumnType in (''CF'', ''CV'') then substr(ColumnFormat, 3, length(ColumnFormat)-3) else ColumnLength end as integer) as character_maximum_length,
                                DecimalTotalDigits as   numeric_precision, 
                                DecimalFractionalDigits as numeric_scale,
                                DecimalFractionalDigits as datetime_precision, 
                                Nullable as nullable 
                        from    DBC.ColumnsV c
                        join    DBC.TablesV t on 
                                c.databaseName=t.DatabaseName AND 
                                c.TableName=t.TableName AND 
                                TableKind=''T''
                        where   table_schema not in (''All'', ''Crashdumps'', ''DBC'', ''dbcmngr'', 
    ''Default'', ''External_AP'', ''EXTUSER'', ''LockLogShredder'', ''PUBLIC'',
    ''Sys_Calendar'', ''SysAdmin'', ''SYSBAR'', ''SYSJDBC'', ''SYSLIB'',
    ''SystemFe'', ''SYSUDTLIB'', ''SYSUIF'', ''TD_SERVER_DB'', ''TDStats'',
    ''TD_SYSGPL'', ''TD_SYSXML'', ''TDMaps'', ''TDPUSER'', ''TDQCD'',
    ''tdwm'', ''SQLJ'', ''TD_SYSFNLIB'', ''SYSSPATIAL'') AND
                                table_schema like '']]..SCHEMA_FILTER..[['' AND
                                table_name like '']]..TABLE_FILTER..[[''
		      ') as tableList order by false
)

, vv_primary_keys_raw as (
	select	]]..exa_upper_begin..[["table_schema"]]..exa_upper_end..[[ as "exa_table_schema", 
			]]..exa_upper_begin..[["table_name"]]..exa_upper_end..[[ as "exa_table_name", 
			]]..exa_upper_begin..[["column_name"]]..exa_upper_end..[[ as "exa_column_name",
			"table_schema" as "table_schema",
	        "table_name" as "table_name",
	        "column_name" as "column_name",
	        "column_position" as "column_position"
	from (
			import from jdbc at ]]..CONNECTION_NAME..[[  
			statement '
			SELECT  DatabaseName as table_schema,
			        TableName as table_name,
			        ColumnName as column_name,
			        ColumnPosition as column_position
			FROM    DBC.IndicesV
			WHERE UniqueFlag = ''Y'' AND IndexType IN (''K'')
			and table_schema not in (''All'', ''Crashdumps'', ''DBC'', ''dbcmngr'', 
			    ''Default'', ''External_AP'', ''EXTUSER'', ''LockLogShredder'', ''PUBLIC'',
			    ''Sys_Calendar'', ''SysAdmin'', ''SYSBAR'', ''SYSJDBC'', ''SYSLIB'',
			    ''SystemFe'', ''SYSUDTLIB'', ''SYSUIF'', ''TD_SERVER_DB'', ''TDStats'',
			    ''TD_SYSGPL'', ''TD_SYSXML'', ''TDMaps'', ''TDPUSER'', ''TDQCD'',
			    ''tdwm'', ''SQLJ'', ''TD_SYSFNLIB'', ''SYSSPATIAL'') AND
			table_schema like '']]..SCHEMA_FILTER..[['' AND
			table_name like '']]..TABLE_FILTER..[['' 
			'
	) as primarykeylist
)

, vv_foreign_keys_raw as (
	select   ]]..exa_upper_begin..[["ChildDB"]]..exa_upper_end..[[ as "exa_table_schema", 
		 ]]..exa_upper_begin..[["ChildTable"]]..exa_upper_end..[[ as "exa_table_name", 
		 ]]..exa_upper_begin..[["ChildKeyColumn"]]..exa_upper_end..[[ as "exa_foreign_key_column",
		 ]]..exa_upper_begin..[["ParentDB"]]..exa_upper_end..[[ as "exa_referenced_table_schema", 
		 ]]..exa_upper_begin..[["ParentTable"]]..exa_upper_end..[[ as "exa_referenced_table_name"
	from (
			import from jdbc at ]]..CONNECTION_NAME..[[  
			statement ' 
			SELECT  ChildDB,
					ChildTable,
			        ChildKeyColumn,
			        ParentDB ,
			        ParentTable 
			FROM    DBC.All_RI_ChildrenV
			WHERE   ChildDB NOT IN (''All'', ''Crashdumps'', ''DBC'', ''dbcmngr'', 
			    ''Default'', ''External_AP'', ''EXTUSER'', ''LockLogShredder'', ''PUBLIC'',
			    ''Sys_Calendar'', ''SysAdmin'', ''SYSBAR'', ''SYSJDBC'', ''SYSLIB'',
			    ''SystemFe'', ''SYSUDTLIB'', ''SYSUIF'', ''TD_SERVER_DB'', ''TDStats'',
			    ''TD_SYSGPL'', ''TD_SYSXML'', ''TDMaps'', ''TDPUSER'', ''TDQCD'',
			    ''tdwm'', ''SQLJ'', ''TD_SYSFNLIB'', ''SYSSPATIAL'') and
			ChildDB like '']]..SCHEMA_FILTER..[['' AND
			ChildTable like '']]..TABLE_FILTER..[['' 
			'
	)

)

, vv_create_schemas as(
	SELECT 'create schema if not exists "' || "exa_table_schema" || '";' as sql_text 
	from vv_columns  
	group by "exa_table_schema" 
	order by "exa_table_schema"
)

, vv_create_tables as (
	select 'create or replace table "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || 
	group_concat(
	case 
	when "data_type" = 'PD' then -- a teradata PERIOD(DATE) is splitted into the beginning and end DATE  
	       '"' ||  "exa_column_name" || '_BEGINNING " DATE,' ||
	       '"' ||  "exa_column_name" || '_END" DATE' 
	when "data_type" in ('PS', 'PM', 'PT', 'PZ')  then  -- a teradata PERIOD(TIMESTAMP) is splitted into the beginning and end TIMESTAMP  
	       '"' ||  "exa_column_name" || '_BEGINNING " TIMESTAMP,' ||
	       '"' ||  "exa_column_name" || '_END" TIMESTAMP'        	       
	else
	       '"' ||  "exa_column_name" || '" ' 
	end
	||  
	case 
	when "data_type" = 'PD' then ''  --Period is already splitted into two dates before
	when "data_type" in ('PS', 'PM', 'PT', 'PZ') then ''  --Period is already splitted into two timestamps before
	when "data_type" = 'DA' then 'DATE' 
	when "data_type" in ('BF', 'BO', 'BV') then 'VARCHAR(100)' --binary data types like BYTE, VARBYTE, BLOB are not supported then
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
	when "data_type" = 'TZ' then 'TIMESTAMP' 
	when "data_type" = 'SZ' then 'TIMESTAMP' 
	when "data_type" = 'CF' then  
                case 
                when nvl("character_maximum_length",2000) > 2000 then 
                'varchar(' ||
                       nvl("character_maximum_length",2000) || ')' 
                else 
                'char(' ||
                       nvl("character_maximum_length",2000) || ')' 
                end 
	when "data_type" = 'I1' then 'DECIMAL(9)'
	when "data_type" = 'I2' then 'DECIMAL(9)'
	when "data_type" = 'I8' then 'DECIMAL(19)' --maybe 18 but can result in errors while importing
	when "data_type" = 'AT' then 'TIMESTAMP' 
	when "data_type" = 'F'  then 'DOUBLE' 
	when "data_type" in( 'CV' , 'JN') then  --Varchar and JSON
	       'varchar(' || case when nvl("character_maximum_length",2000000) > 2000000 then 
	       2000000 
	       else 
	               nvl("character_maximum_length",2000000) 
               end || ')' 
	when "data_type" = 'I'  then 'DECIMAL(10)' --maybe 9 but can result in errors while importing
	when "data_type" = 'N'  then  --Number type 
	       case when "numeric_precision" is null or "numeric_precision" > 36 or "numeric_precision" = -128 then 
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
        when "data_type" in ('A1','AN')  then --ARRAY Datatype  
           'VARCHAR(64000)'
	else '/*UNKNOWN_DATATYPE:' || "data_type" || '*/ varchar(2000000)' 
	end || case when "nullable" = 'N' then ' NOT NULL ' else '' end
	
	order by       "ordinal_position") || ');' as sql_text
	from           vv_columns  
	group by       "exa_table_schema", "exa_table_name"
	order by       "exa_table_schema","exa_table_name"
)

, vv_primary_keys as (
	select 	'ALTER TABLE "' || "exa_table_schema" || '"."' || "exa_table_name" || '" ADD CONSTRAINT PRIMARY KEY (' || 
			group_concat('"'||  "exa_column_name" || '"'  order by "column_position")  || ') ;' as sql_text
	from vv_primary_keys_raw   
	group by "exa_table_schema", "exa_table_name"
	order by "exa_table_schema","exa_table_name"
)

, vv_foreign_keys as (
	select 'ALTER TABLE "' || "exa_table_schema" || '"."' || "exa_table_name" ||
	 '" ADD FOREIGN KEY (' || '"'||  "exa_foreign_key_column" || '") REFERENCES "' || "exa_referenced_table_schema" || '"."' || "exa_referenced_table_name" || '" DISABLE ;' as sql_text
	from vv_foreign_keys_raw   
	order by "exa_table_schema","exa_table_name"
)

, vv_imports as (
	select 	'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select ' || 
			group_concat( 
				case 
				when "data_type" = 'DA' then "column_name_delimited"
				when "data_type" = 'D'  then "column_name_delimited"
				when "data_type" = 'TS' then "column_name_delimited"
				when "data_type" = 'CF' then "column_name_delimited"
				when "data_type" = 'I1' then "column_name_delimited"
				when "data_type" = 'I2' then "column_name_delimited"
				when "data_type" = 'I8' then "column_name_delimited"
				when "data_type" = 'AT' then "column_name_delimited"
				when "data_type" = 'F'  then "column_name_delimited"
				when "data_type" = 'CV' then "column_name_delimited"
				when "data_type" = 'I'  then "column_name_delimited"
				when "data_type" = 'N'  then "column_name_delimited"
				when "data_type" in ('A1','AN')  then 'cast(' || "column_name_delimited" || ' as varchar(64000)) '  --array datatypes are casted to a varchar in Teradata
				when "data_type" in ('BF', 'BO', 'BV') then '''''NOT SUPPORTED''''' --binary data types (BYTE, VARBYTE, BLOB) are not supported
				when "data_type" = 'JN'  then 'CAST(' || "column_name_delimited" ||  ' AS CLOB ) ' --json (max length in Exasol is 2000000 as it is stored as varchar)  
				when "data_type" = 'PD'  then  'BEGIN('|| "column_name_delimited" || ') , END(' ||  "column_name_delimited" || ')'  --Period(Date) split into begin and end date
				when "data_type" in ('PS', 'PM')  then  'CAST(  BEGIN('|| "column_name_delimited" || ') AS TIMESTAMP ) , CAST ( END(' ||  "column_name_delimited" || ') AS TIMESTAMP ) '  --Period(Timestamp) split into begin and end timestamp  
				when "data_type" in ('PT', 'PZ')  then  'CAST(  BEGIN('|| "column_name_delimited" || ') AS TIME ) , CAST ( END(' ||  "column_name_delimited" || ') AS TIME ) '  --Period(Time) split into begin and end time	
				when "data_type" = 'TZ' then  'cast(' || "column_name_delimited" || ' AS TIME)'  --time with time zone
				when "data_type" = 'SZ' then  'cast(' || "column_name_delimited" || ' AS TIMESTAMP)'  --timestamp with time zone
				when "data_type" = 'YR'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL YEAR  TO MONTH ) AS VARCHAR(50))'  --Interval Year
				when "data_type" = 'YM'  then 'cast('|| "column_name_delimited" || ' AS VARCHAR(50) )'  --Interval Year to Month
				when "data_type" = 'MO'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL YEAR  TO MONTH ) AS VARCHAR(50))'  --Interval Month
				when "data_type" = 'DY'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) ' --Interval Day
				when "data_type" = 'DH'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Day to hour
				when "data_type" = 'DM'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Day to minute
				when "data_type" = 'DS'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval day to second
				when "data_type" = 'HR'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Hour
				when "data_type" = 'HM'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) '  --Interval Day to minute
				when "data_type" = 'HS'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval day to second
				when "data_type" = 'MI'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND) AS VARCHAR(50)) ' --Interval Minute
				when "data_type" = 'MS'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval minute to second
				when "data_type" = 'SC'  then 'cast(cast('|| "column_name_delimited" || ' AS INTERVAL DAY (4)  TO SECOND (' || "numeric_scale" || ')) AS VARCHAR(50)) '  --Interval Second
				else "column_name_delimited"
				end
				order by "ordinal_position"
			) || 
			' from ' || "table_schema"|| '.' || "table_name"|| ''';' as sql_text
	from vv_columns 
	group by "exa_table_schema","exa_table_name", "table_schema","table_name"
	order by "exa_table_schema","exa_table_name", "table_schema","table_name"
)

, vv_checks_expr as (
	select  "db_system", c."exa_table_schema", c."exa_table_name", c."exa_column_name", c."column_name_delimited", c."table_schema", c."table_name", c."ordinal_position", c."data_type", c."nullable",
	        count(case when p."exa_column_name" is not null then 1 end) over (partition by c."exa_table_schema", c."exa_table_name") as "cnt_pk",
	        case when p."exa_column_name" is not null then true else false end "is_pk",
	        p."column_position" as "column_position_pk",
	        "metric_id",
	        'DATABASE_MIGRATION' as "metric_schema",
	        c."exa_table_name" || '_MIG_CHK' as "metric_table",
	        case    when c."ordinal_position" = 1 and "metric_id" = 0 then 'cast(count(*) as decimal(36,0))'
	        		when "nullable" = 'Y' and "metric_id" = 1 then 'cast(count(case when ' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ' is null then 1 end) as decimal(36,0))'
	        		when c."data_type" in ( 'DA',   --DATE 
	                                        'AT',   --TIME
	                                        'TZ',   --TIME WITH TIME ZONE
	                                        'TS',   --TIMESTAMP
	                                        'SZ',   --TIMESTAMP WITH TIME ZONE
	                                        
	                                        'I',    --INTEGER
	                                        'I1',   --BYTEINT
	                                        'I2',   --SMALLINT
	                                        'I8',   --BIGINT
	                                        'F',    --DOUBLE PRECISION 
	                                        'N',    --NUMBER
	                                        'D'     --DECIMAL
	                                        ) then
	                		case 	when "metric_id" = 2 then
	                                        case when c."data_type" in ('I', 'I1','I2','I8','F','N','D') then 'cast(' end ||
	                                        'min(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ')' ||
	                                        case    when c."data_type" in ('I1', 'I2', 'I8', 'I') then ' as decimal(20,0))' 
	                                                when c."data_type" = 'F' or c."numeric_precision" > 36 or c."numeric_precision" = -128 then ' as double precision)' 
	                                                when c."data_type" in ('N', 'D') and c."numeric_precision" > 0 and c."numeric_scale" >= 0 then ' as decimal(' || c."numeric_precision" || ', ' || c."numeric_scale" || '))'
	                                        end
	                                when "metric_id" = 3 then
	                                        case when c."data_type" in ('I', 'I1','I2','I8','F','N','D') then 'cast(' end ||
	                                        'max(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ')' ||
	                                        case    when c."data_type" in ('I1', 'I2', 'I8', 'I') then ' as decimal(20,0))' 
	                                                when c."data_type" = 'F' or c."numeric_precision" > 36 or c."numeric_precision" = -128 then ' as double precision)' 
	                                                when c."data_type" in ('N', 'D') and c."numeric_precision" > 0 and c."numeric_scale" >= 0 then ' as decimal(' || c."numeric_precision" || ', ' || c."numeric_scale" || '))'
	                                        end
	                                when "metric_id" = 4 then
	                        				'cast(count(distinct(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ')) as decimal(36,0))'
	                				when "metric_id" = 5 then
	                                        case    when c."data_type" in ('D', 'I1', 'I2', 'I8', 'F', 'I', 'N') then
	                                                		'cast(avg(cast(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ' as double precision)) as double precision)'    --avoiding numeric overflow by casting the column first , second cast is to make sure the data types match after the import                           
	                                        end
	                        end
	
	                when c."data_type" in ( 'PD',   --PERIOD(DATE)
	                                        'PT',   --PERIOD(TIME(n))
	                                        'PZ',   --PERIOD(TIME(n) WITH TIME ZONE)
	                                        'PS',   --PERIOD(TIMESTAMP(n))
	                                        'PM'    --PERIOD(TIMESTAMP(n) WITH TIME ZONE)
	                                        ) then
	                        case    when "db_system" = 'Exasol' then
	                        				case	when "metric_id" = 2 then 'min("' || c."exa_column_name" || '_BEGINNING")'
	                                        		when "metric_id" = 3 then 'max("' || c."exa_column_name" || '_BEGINNING")'
	                                        		when "metric_id" = 4 then 'min("' || c."exa_column_name" || '_END")'
	                                        		when "metric_id" = 5 then 'max("' || c."exa_column_name" || '_END")'
	                                        		when "metric_id" = 6 then 'cast(count(distinct("' || c."exa_column_name"  || '_BEGINNING", "' || c."exa_column_name" || '_END")) as decimal(36,0))'
	                                		end
	                                when "db_system" = 'Teradata' then
	                                		case	when "metric_id" = 2 then 'min(begin(' || c."column_name_delimited" || '))'
	                                        		when "metric_id" = 3 then 'max(begin(' || c."column_name_delimited" || '))'
	                                        		when "metric_id" = 4 then 'min(end(' || c."column_name_delimited" || '))'
	                                        		when "metric_id" = 5 then 'max(end(' || c."column_name_delimited" || '))'
	                                        		when "metric_id" = 6 then 'cast(count(distinct(' || c."column_name_delimited" || ')) as decimal(36,0))'
	                                		end
	                        end
	                
	                when c."data_type" in ( 'CF',   --CHARACTER (fixed)
	                                        'CV'    --CHARACTER (varying)
	                                        ) then
	                        case    when not(c."data_type" = 'CF' and "character_maximum_length" > 2000) then
	                        				case 	when "metric_id" = 2 then 'cast(min(length(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ')) as decimal(36,0))'
	                        						when "metric_id" = 3 then 'cast(max(length(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end || ')) as decimal(36,0))'
	                						end
	                        		when "metric_id" = 4 then 'cast(count(distinct(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" || ' (casespecific)' end || ')) as decimal(36,0))'
	                        end 
	                        
	                when c."data_type" in ( 'YR',   --interval year
	                                        'YM',   --interval year to month
	                                        'MO',   --interval month
	                                        'DY',   --interval day
	                                        'DH',   --interval day to hour
	                                        'DM',   --interval day to minute
	                                        'DS',   --interval day to second
	                                        'HR',   --interval hour
	                                        'HM',   --interval hour to minute
	                                        'HS',   --interval hour to second
	                                        'MI',   --interval minute
	                                        'MS',   --interval minute to second
	                                        'SC'    --interval second
	                                        ) then
	                		case	when "metric_id" = 2 then 'cast(count(distinct(' || case when "db_system" = 'Exasol' then '"' || c."exa_column_name" || '"' else c."column_name_delimited" end  || ')) as decimal(36,0))'
	                		end
	                when "metric_id" = 1 then 'cast(''' || case when "db_system" = 'Teradata' then '''' end || 'data type "' || c."data_type" ||'" not checked' || case when "db_system" = 'Teradata' then '''' end || ''' as varchar(40))'
	        end     "metric_column_expression",
	        
	        case 	when c."ordinal_position" = 1 and "metric_id" = 0 then '"CNT"'
	        		when "nullable" = 'Y' and "metric_id" = 1 then '"' || c."exa_column_name" || '_CNT_NUL"'
	        		when c."data_type" in ( 'DA',   --DATE 
	                                        'AT',   --TIME
	                                        'TZ',   --TIME WITH TIME ZONE
	                                        'TS',   --TIMESTAMP
	                                        'SZ',   --TIMESTAMP WITH TIME ZONE
	                                        
	                                        'I',    --INTEGER
	                                        'I1',   --BYTEINT
	                                        'I2',   --SMALLINT
	                                        'I8',   --BIGINT
	                                        'F',    --DOUBLE PRECISION 
	                                        'N',    --NUMBER
	                                        'D'     --DECIMAL
	                                        ) then
	                		case 	when "metric_id" = 2 then '"' || c."exa_column_name" || '_MIN"'
	                				when "metric_id" = 3 then '"' || c."exa_column_name" || '_MAX"'
	                				when "metric_id" = 4 then '"' || c."exa_column_name" || '_CNT_DST"'
	                				when "metric_id" = 5 then '"' || c."exa_column_name" || '_AVG"'
	        				end 
		 			when c."data_type" in ( 'PD',   --PERIOD(DATE)
	                                        'PT',   --PERIOD(TIME(n))
	                                        'PZ',   --PERIOD(TIME(n) WITH TIME ZONE)
	                                        'PS',   --PERIOD(TIMESTAMP(n))
	                                        'PM'    --PERIOD(TIMESTAMP(n) WITH TIME ZONE)
	                                        ) then
	                        case	when "metric_id" = 2 then '"' || c."exa_column_name" || '_BEGINNING_MIN"'
	                        		when "metric_id" = 3 then '"' || c."exa_column_name" || '_BEGINNING_MAX"'
	                        		when "metric_id" = 4 then '"' || c."exa_column_name" || '_END_MIN"'
	                        		when "metric_id" = 5 then '"' || c."exa_column_name" || '_END_MAX"'
	                        		when "metric_id" = 6 then '"' || c."exa_column_name" || '_CNT_DST"'
	                		end
	        		 when c."data_type" in ( 'CF',   --CHARACTER (fixed)
	                                         'CV'    --CHARACTER (varying)
	                                        ) then
	                        case    when not(c."data_type" = 'CF' and "character_maximum_length" > 2000) then
	                        				case 	when "metric_id" = 2 then '"' || c."exa_column_name" || '_MIN_LEN"'
	                        						when "metric_id" = 3 then '"' || c."exa_column_name" || '_MAX_LEN"'
	                						end
	                        		when "metric_id" = 4 then '"' || c."exa_column_name" || '_CNT_DST"'
	                        end
	                when c."data_type" in ( 'YR',   --interval year
	                                        'YM',   --interval year to month
	                                        'MO',   --interval month
	                                        'DY',   --interval day
	                                        'DH',   --interval day to hour
	                                        'DM',   --interval day to minute
	                                        'DS',   --interval day to second
	                                        'HR',   --interval hour
	                                        'HM',   --interval hour to minute
	                                        'HS',   --interval hour to second
	                                        'MI',   --interval minute
	                                        'MS',   --interval minute to second
	                                        'SC'    --interval second
	                                        ) then
	        				case	when "metric_id" = 2 then '"' || c."exa_column_name" || '_CNT_DST"'
	                		end
	                when "metric_id" = 1 then 'not checked ''' || c."exa_column_name" || ''''
	        end		"metric_column_name"
	from vv_columns c 
	left join vv_primary_keys_raw p
	on c."exa_table_schema" = p."exa_table_schema"
	and c."exa_table_name" = p."exa_table_name" 
	and c."exa_column_name" = p."exa_column_name"
	  , (select 'Teradata' "db_system" union all select 'Exasol' "db_system")
	  , (select level -1 "metric_id" from dual connect by level <= 7)
	where local."metric_column_expression" is not null
	and 1 = ]] .. check_control .. [[
	order by c."exa_table_name", c."ordinal_position" asc nulls first, "metric_id"
)

, vv_checks as (
        select  'create or replace table "' || "exa_table_schema" || '"."' || "metric_table" || '" as ' ||  
                listagg("check_sql",  ' union all ' ) within group(order by case when "db_system" = 'Exasol' then 1 else 2 end) || ';' as sql_text
        from (
                select  "db_system", "exa_table_schema", "exa_table_name", "metric_table",
                        case when "db_system" = 'Teradata' then 'select * from (import from jdbc at ]] .. CONNECTION_NAME .. [[ statement '''  end ||
                        'select cast(''' || case when "db_system" = 'Teradata' then '''' end || "db_system" || case when "db_system" = 'Teradata' then '''' end  || ''' as varchar(20)) as "DB_SYSTEM", ' || 
                        listagg("metric_column_expression" || ' as ' || "metric_column_name", ', ') within group(order by "ordinal_position", "metric_id") || 
                        ' from ' || 
                        case when "db_system" = 'Exasol' then '"' || "exa_table_schema" || '"."' || "exa_table_name" || '"' else '"' || "table_schema" || '"."' || "table_name" || '"' end ||
                        case when "db_system" = 'Teradata' then ''') ' end 
                        as "check_sql"
                from vv_checks_expr
                group by "db_system", "exa_table_schema", "exa_table_name", "table_schema", "table_name", "metric_table"
        )
        group by "exa_table_schema", "metric_table"
        order by "exa_table_schema", "metric_table"
)

, vv_check_summary as (       
        select  'create or replace table database_migration.migration_check (schema_name varchar(128), table_name varchar(128), column_name varchar(128), exasol_metric varchar(50), teradata_metric varchar(50), check_timestamp timestamp default current_timestamp);' as sql_text
        union all
        select  'insert into "DATABASE_MIGRATION"."MIGRATION_CHECK" (schema_name, table_name, column_name, exasol_metric, teradata_metric) ' /*|| chr(10)*/
                || listagg(sql_text, '') within group(order by case when db_name = 'Exasol' then 1 else 2 end) || ' ' /*|| chr(10)*/ 
                || 'select e.schema_name, e.table_name, e.column_name, e.exasol_metric, t.teradata_metric from exasol e join teradata t on e.schema_name = t.schema_name and e.table_name = t.table_name and e.column_name = t.column_name; ' as sql_text
        from (
        
                select  db_name, column_schema, column_table,
                        case when db_name = 'Exasol' then 'with ' else ', ' end || db_name || ' as ( ' /*|| chr(10)*/
                        || listagg(case when column_name != 'DB_SYSTEM' then 'select ''' || column_schema || ''' as schema_name, ''' || column_table || ''' as table_name, ''' || column_name || ''' as column_name, to_char("' || column_name || '") as ' || db_name || '_metric from "' || column_schema || '"."' || column_table || '" where DB_SYSTEM = ''' || db_name || '''' end, ' union all ' /* || chr(10)*/)
                        /*|| chr(10)*/
                        || ' )'  as sql_text
                from exa_all_columns, (select 'Exasol' db_name union all select 'Teradata' db_name), (select distinct "exa_table_schema", "exa_table_name" from vv_columns)
                where column_schema = "exa_table_schema"
                and column_table = "exa_table_name" || '_MIG_CHK'
                group by db_name, column_schema, column_table
                order by case when db_name = 'Exasol' then 1 else 2 end
        
        )
        group by column_schema, column_table
        union all
        select 'select * from database_migration.migration_check where exasol_metric != teradata_metric;' as sql_text
        from dual
)
select * from vv_create_schemas
UNION ALL
select * from vv_create_tables
UNION ALL
select * from vv_imports
UNION ALL
select * from vv_checks
UNION ALL
select * from vv_check_summary
UNION ALL
select * from vv_primary_keys
UNION ALL
select * from vv_foreign_keys
]],{})
return(res)
/
;
-- !!! Important: Please upload the Teradata JDBC-Driver via EXAOperation (Webinterface) !!!
-- !!! you can see a similar example for Oracle here: https://www.exasol.com/support/browse/SOL-179 !!!

-- Create a connection to the Teradata database
create or replace connection teradata_db to 'jdbc:teradata://192.168.56.103/CHARSET=UTF16' user 'dbc' identified by 'dbc';
-- Depending on your Teradata installation, CHARSET=UTF16 instead of CHARSET=UTF8 could be the better choice - otherwise you get errors like this one:
-- [42636] ETL-3003: [Column=5 Row=0] [String data right truncation. String length exceeds limit of 2 characters] (Session: 1611884537138472475)
-- In that case, configure your connection like this:
-- create connection teradata_db to 'jdbc:teradata://some.teradata.host.internal/CHARSET=UTF16' user 'db_username' identified by 'exasolRocks!';



IMPORT FROM JDBC AT teradata_db
STATEMENT 'SELECT ''connection to teradata works''';


-- Finally start the import process
execute script database_migration.TERADATA_TO_EXASOL(
    'TERADATA_DB'	-- name of your dataabase connection
    ,true        	-- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true
    ,'RETAIL_2020'	-- schema filter --> '%' to load all schemas except 'DBC' / '%pub%' to load all schemas like '%pub%'
    ,'%'			--'DimCustomer' -- table filter --> '%' to load all tables
    ,true			-- boolean flag to create checking tables
)
;