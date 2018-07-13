create schema database_migration;

/* 
	This script will generate create schema, create table and create import statements 
	to load all needed data from an oracle database. Automatic datatype conversion is
	applied whenever needed. Feel free to adjust it. 
*/

--/
create or replace script database_migration.ORACLE_TO_EXASOL (
CONNECTION_NAME				 -- name of the database connection inside exasol -> e.g. mysql_db
,IDENTIFIER_CASE_INSENSITIVE -- TRUE if identifiers should be put uppercase
,SCHEMA_FILTER               -- filter for the schemas to generate and load, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
,TABLE_FILTER                -- filter for the tables to generate and load, e.g. 'my_table', 'my%', 'table1, table2', '%'
) RETURNS TABLE 
AS

-- Functions
function string.startsWith(String,word)
   return string.sub(String,1,string.len(word))==word
end


function get_connection_type_by_testing(CONNECTION_NAME)
	-- TEST OCI/ORA
	success, res = pquery([[
	
	select * from(
			import from ora at ::c
			statement 'select owner from ALL_TAB_COLUMNS'
					);]], {c=CONNECTION_NAME})
	if success then
		return 'ORA' 
	end

	-- TEST JDBC
	success, res = pquery([[
	
	select * from(
			import from jdbc at ::c
			statement 'select owner from ALL_TAB_COLUMNS'
					);]], {c=CONNECTION_NAME})
	if success then
		return 'JDBC' 
	end
	return 'unknown'
end


function get_connection_type(CONNECTION_NAME)
	CONNECTION_TYPE='unknown'
	-- check system table for connection type first
	success, res = pquery([[select CONNECTION_STRING from SYS.EXA_DBA_CONNECTIONS
		where CONNECTION_NAME = :c ]] , {c=CONNECTION_NAME})

	output(res.statement_text)
	
	if success then
		if #res == 0 then
			error([[The connection ]]..CONNECTION_NAME..[[ doesn't exist, please try again with a valid connection name]])
		end
		if string.startsWith(string.upper(res[1][1]), 'JDBC') then 
			CONNECTION_TYPE = 'JDBC'
		else 
			CONNECTION_TYPE = 'ORA'
		end

	else -- if user can't access this table --> try oci and jdbc
		output([[Can't access table SYS.EXA_DBA_CONNECTIONS ... will try to determine connection type by trying it out ]])
		CONNECTION_TYPE = get_connection_type_by_testing(CONNECTION_NAME)
	end
	
	output('Connection detected as '..CONNECTION_TYPE..' connection')
	-- error handling
	if CONNECTION_TYPE == 'unknown' then
		error([[The connection ]]..CONNECTION_NAME..[[ seems to fit neither an JDBC nor an OCI connection pattern, please verify that ]]..CONNECTION_NAME..[[ is a valid OCI/JDBC connection]])
	end
	return CONNECTION_TYPE
end

-- Actual script

-- check whether connection is OCI or JDBC Connection
CONNECTION_TYPE = get_connection_type(CONNECTION_NAME)



exa_upper_begin=''
exa_upper_end=''
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin='upper('
	exa_upper_end=')'
end

if string.match(SCHEMA_FILTER, '%%') then	
	SCHEMA_STR = [[like ('']]..SCHEMA_FILTER..[['')]]		
else	
	SCHEMA_STR = [[in ('']]..SCHEMA_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]		
end

if string.match(TABLE_FILTER, '%%') then	
	TABLE_STR = [[like ('']]..TABLE_FILTER..[['')]]			
else
	TABLE_STR = [[in ('']]..TABLE_FILTER:gsub("^%s*(.-)%s*$", "%1"):gsub('%s*,%s*',"'',''")..[['')]]		
end

--check weather identity_column exists in all_tab_columns for this oracle version
success, res = pquery([[

select * from(
		import from ]]..CONNECTION_TYPE..[[ at ::c statement
			'
			select 
			COLUMN_NAME
			 from
			ALL_TAB_COLUMNS
			where
			TABLE_NAME = ''ALL_TAB_COLUMNS'' and 
			COLUMN_NAME = ''IDENTITY_COLUMN''
			'
)

]],{c=CONNECTION_NAME})

if not success then error(res.error_message) end

all_tab_cols = [[]]

if #res == 0 then --no identity column
	all_tab_cols = exa_upper_begin..[[ owner ]]..exa_upper_end..[[ as EXA_SCHEMA_NAME , owner , table_name, ]]..exa_upper_begin..[[ table_name ]]..exa_upper_end..[[ as EXA_TABLE_NAME , COLUMN_NAME, ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[  as EXA_COLUMN_NAME, data_type, cast(data_length as decimal(9,0)) data_length, cast(data_precision as decimal(9,0)) data_precision, cast(data_scale as decimal(9,0)) data_scale, cast(char_length as decimal(9,0)) char_length , nullable, cast(column_id as decimal(9,0)) column_id, null identity_column]]
else
  	all_tab_cols = exa_upper_begin..[[ owner ]]..exa_upper_end..[[ as EXA_SCHEMA_NAME , owner , table_name, ]]..exa_upper_begin..[[ table_name ]]..exa_upper_end..[[ as EXA_TABLE_NAME , COLUMN_NAME, ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[  as EXA_COLUMN_NAME, data_type, cast(data_length as decimal(9,0)) data_length, cast(data_precision as decimal(9,0)) data_precision, cast(data_scale as decimal(9,0)) data_scale, cast(char_length as decimal(9,0)) char_length , nullable, cast(column_id as decimal(9,0)) column_id, identity_column]]
end

success, res = pquery([[with ora_cols as( 

	select * from(
		import from ]]..CONNECTION_TYPE..[[ at ::c
		statement 'select ]]..all_tab_cols..[[  from all_tab_columns where owner ]]..SCHEMA_STR..[[ and table_name ]]..TABLE_STR..[['
				)
			),
	ora_base as ( --cast to correct types
		SELECT
			EXA_SCHEMA_NAME, 
			OWNER, 
			TABLE_NAME, 
			EXA_TABLE_NAME, 
			COLUMN_NAME, 
			EXA_COLUMN_NAME, 
			DATA_TYPE, 
			cast(DATA_LENGTH as integer) DATA_LENGTH, 
			cast(DATA_PRECISION as integer) DATA_PRECISION, 
			cast(DATA_SCALE as integer) DATA_SCALE, 
			cast(CHAR_LENGTH as integer) CHAR_LENGTH, 
			NULLABLE, 
			cast(COLUMN_ID as integer) COLUMN_ID, 
			IDENTITY_COLUMN
		FROM
			ora_cols
	),
	nls_format as (
		select * from (import from ]]..CONNECTION_TYPE..[[ at ::c statement 'select * from nls_database_parameters where parameter in (''NLS_TIMESTAMP_FORMAT'',''NLS_DATE_FORMAT'',''NLS_DATE_LANGUAGE'',''NLS_CHARACTERSET'')')
	),
	cr_schema as (
		with EXA_SCHEMAS as (select distinct EXA_SCHEMA_NAME as EXA_SCHEMA from ora_base )
			select 'create schema if not exists "' ||  EXA_SCHEMA ||'";' as cr_schema from EXA_SCHEMAS
	),
	cr_tables as (
		select 'create or replace table "' || EXA_SCHEMA_NAME || '"."' || EXA_TABLE_NAME || '" (' || cols || '); ' || cols2 || ''
as tbls from 
(select EXA_SCHEMA_NAME, EXA_TABLE_NAME, 
		group_concat( 
		case 
			when data_type in ('CHAR', 'NCHAR') then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'char(' || char_length || ')'
			when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(' || char_length || ')'
			when data_type = 'CLOB' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(2000000)'
			when data_type = 'XMLTYPE' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(2000000)'
			when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  case when data_scale > 36 then 'DECIMAL(' || 36 || ',' || 36 || ')' when data_precision > 36 and data_scale <= 36 then 'DECIMAL(' || 36 || ',' || data_scale || ')' when data_precision <= 36 and data_scale > data_precision then  'DECIMAL(' || data_scale || ',' || data_scale || ')' else 'DECIMAL(' || data_precision || ',' || data_scale || ')' end   
			when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  case when data_scale > 36 then 'DECIMAL(' || 36 || ',' || 36 || ')' when data_precision > 36 and data_scale <= 36 then 'DECIMAL(' || 36 || ',' || data_scale || ')' when data_precision <= 36 and data_scale > data_precision then  'DECIMAL(' || data_scale || ',' || data_scale || ')' else 'DECIMAL(' || data_precision || ',' || data_scale || ')' end
			when data_type = 'NUMBER' and (data_length is not null and data_precision is null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'integer' 
			when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'double precision'
			when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'double precision'
			when data_type = 'DATE' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'timestamp'
			when data_type like 'TIMESTAMP(%)%' or data_type like 'TIMESTAMP' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'timestamp'
			when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'timestamp' 
			when data_type = 'BOOLEAN' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'boolean'
			-- Fallback for unsupported data types
			-- else '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(2000000) /* UNSUPPORTED DATA TYPE : ' || data_type || ' */ '
			end
			
		|| case when identity_column='YES' then ' IDENTITY' end
		|| case when nullable='N' then ' NOT NULL' end
		
	order by column_id SEPARATOR ', ')
	as cols,
                group_concat( 
                        case 
                        when data_type not in ('CHAR', 'NCHAR', 'VARCHAR', 'VARCHAR2', 'NVARCHAR2', 'CLOB', 'XMLTYPE', 'DECIMAL', 'NUMBER', 'DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'DATE', 'BOOLEAN', 'TIMESTAMP') and data_type not like 'TIMESTAMP(%)%' and data_type not like 'TIMESTAMP%WITH%TIME%ZONE%'
                        then '--UNSUPPORTED DATA TYPE : "'|| EXA_COLUMN_NAME || '" ' || data_type || ''
                        end
                ) 
	as cols2 
        from ora_base group by EXA_SCHEMA_NAME, EXA_TABLE_NAME)
	),
	cr_import_stmts as (
		select 'import into "' || EXA_SCHEMA_NAME ||'"."' || EXA_TABLE_NAME || '"( ' || 
		group_concat( 
		case 
			when data_type in ('CHAR', 'NCHAR') then '"' || EXA_COLUMN_NAME || '"' 
			when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then '"' || EXA_COLUMN_NAME || '"' 
			when data_type = 'CLOB' then '"' || EXA_COLUMN_NAME || '"' 
			when data_type = 'XMLTYPE' then '"' || EXA_COLUMN_NAME || '"' 
			when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' 
			when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"'  
			when data_type = 'NUMBER' and (data_length is not null and data_precision is null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' 
			when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then '"' || EXA_COLUMN_NAME || '"' 
			when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then '"' || EXA_COLUMN_NAME || '"' 
			when data_type = 'DATE' then '"' || EXA_COLUMN_NAME || '"' 
			when data_type like 'TIMESTAMP(%)%' or data_type like 'TIMESTAMP' then '"' || EXA_COLUMN_NAME || '"' 
			when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then '"' || EXA_COLUMN_NAME || '"' 
			when data_type = 'BOOLEAN' then '"' || EXA_COLUMN_NAME || '"' 
			/* else '--UNSUPPORTED DATATYPE IN COLUMN ' || COLUMN_NAME || ' Oracle Datatype: ' || data_type  */
		end
				
	order by column_id SEPARATOR ', 
	'
	)
|| 

') from ]]..CONNECTION_TYPE..[[ at ]]..CONNECTION_NAME..[[ statement 
''select 
' || 
		group_concat(
		case 
			when data_type in ('CHAR', 'NCHAR') then '"' || column_name || '"' 
			when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then '"' || column_name || '"' 
			when data_type = 'CLOB' then '"' || column_name || '"' 
			when data_type = 'XMLTYPE' then '"' || column_name || '"' 
			when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) then '"' || column_name || '"' 
			when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) then '"' || column_name || '"'  
			when data_type = 'NUMBER' and (data_length is not null and data_precision is null and data_scale is not null) then '"' || column_name  || '"'
			when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then '"' || column_name || '"' 
			when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then 'cast("' || column_name || '" as DOUBLE PRECISION)' 
			when data_type = 'DATE' then '"' || column_name || '"' 
			when data_type like 'TIMESTAMP(%)' or data_type like 'TIMESTAMP' then '"' || column_name || '"' 
			when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then 'cast("' || column_name || '" as TIMESTAMP)' 
			when data_type = 'BOOLEAN' then '"' || column_name || '"' 
			/* else '--UNSUPPORTED DATATYPE IN COLUMN ' || column_name || ' Oracle Datatype: ' || data_type  */
		end
			
	order by column_id SEPARATOR ', 
	'
	)

|| ' from ' ||  '"' || owner || '"."' || table_name || '"' || ''';'  as imp from ora_base group by owner, table_name, EXA_SCHEMA_NAME, EXA_TABLE_NAME
	)
select sql_text from (
select 1 as ord_hlp,'-- session parameter values are being taken from Oracle systemwide database_parameters and converted. However these should be confirmed before use.' as sql_text
union all
select 2, '-- Oracle DB''s NLS_CHARACTERSET is set to : ' || "VALUE" from nls_format where "PARAMETER"='NLS_CHARACTERSET'
union all
select 3,'-- ALTER SESSION SET NLS_DATE_LANGUAGE=''' || "VALUE" || ''';' from nls_format where "PARAMETER"='NLS_DATE_LANGUAGE'
union all
select 4,'-- ALTER SESSION SET NLS_DATE_FORMAT=''' || replace("VALUE",'R','Y') || ''';' from nls_format where "PARAMETER"='NLS_DATE_FORMAT'
union all
select 5,'-- ALTER SESSION SET NLS_TIMESTAMP_FORMAT=''' || replace(regexp_replace("VALUE",'XF+','.FF6'),'R','Y') || ''';' from nls_format where "PARAMETER"='NLS_TIMESTAMP_FORMAT'
union all
select 6,a.* from cr_schema a
union all
select 7,b.* from cr_tables b
where b.TBLS not like '%();%'
union all
select 8,c.* from cr_import_stmts c
where c.IMP not like '%( ) from%'
) order by ord_hlp
]],{c=CONNECTION_NAME, s=SCHEMA_FILTER, t=TABLE_FILTER})

if not success then error(res.error_message) end

return(res)
/

-- For JDBC Connection
CREATE CONNECTION JDBC_ORACLE  --Install JDBC driver first via EXAoperation, see https://www.exasol.com/support/browse/SOL-179
	TO 'jdbc:oracle:thin:@//192.168.99.100:1521/xe'
	USER 'system'
	IDENTIFIED BY '********';

EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('JDBC_ORACLE', true, '%APEX%','%');

-- For OCI Connection
CREATE OR REPLACE CONNECTION OCI_ORACLE
	TO '192.168.99.100:1521/xe'
	USER 'system'
IDENTIFIED BY '********';

EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('OCI_ORACLE', true, '%APEX%','%');
