create schema database_migration;

create or replace script database_migration.ORACLE_TO_EXASOL (
CONNECTION_NAME
,SCHEMA_FILTER 					-- filter for the schemas to generate and load, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
,TABLE_FILTER  					-- filter for the tables to generate and load, e.g. 'my_table', 'my%', 'table1, table2', '%'
,IDENTIFIER_CASE_INSENSITIVE 	-- TRUE if identifiers should be put uppercase
) RETURNS TABLE 
AS

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
		import from jdbc at ::c statement
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
	all_tab_cols = exa_upper_begin..[[ owner ]]..exa_upper_end..[[ as EXA_SCHEMA_NAME , owner , table_name, ]]..exa_upper_begin..[[ table_name ]]..exa_upper_end..[[ as EXA_TABLE_NAME , COLUMN_NAME, ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[  as EXA_COLUMN_NAME, data_type, cast(data_length as decimal(9,0)) data_length, cast(data_precision as decimal(9,0)) data_precision, cast(data_scale as decimal(9,0)) data_scale, cast(char_length as decimal(9,0)) char_length , nullable, cast(column_id as decimal(9,0)) column_id, data_default, null identity_column]]
else
  	all_tab_cols = exa_upper_begin..[[ owner ]]..exa_upper_end..[[ as EXA_SCHEMA_NAME , owner , table_name, ]]..exa_upper_begin..[[ table_name ]]..exa_upper_end..[[ as EXA_TABLE_NAME , COLUMN_NAME, ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[  as EXA_COLUMN_NAME, data_type, cast(data_length as decimal(9,0)) data_length, cast(data_precision as decimal(9,0)) data_precision, cast(data_scale as decimal(9,0)) data_scale, cast(char_length as decimal(9,0)) char_length , nullable, cast(column_id as decimal(9,0)) column_id, data_default, identity_column]]
end

success, res = pquery([[with ora_cols as( 

	select * from(
		import from jdbc at ::c
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
			DATA_DEFAULT, 
			IDENTITY_COLUMN
		FROM
			ora_cols
	),
	nls_format as (
		select * from (import from jdbc at ::c statement 'select * from nls_database_parameters where parameter in (''NLS_TIMESTAMP_FORMAT'',''NLS_DATE_FORMAT'',''NLS_DATE_LANGUAGE'',''NLS_CHARACTERSET'')')
	),
	cr_schema as (
		with EXA_SCHEMAS as (select distinct EXA_SCHEMA_NAME as EXA_SCHEMA from ora_base )
			select 'create schema "' ||  EXA_SCHEMA ||'";' as cr_schema from EXA_SCHEMAS
	),
	cr_tables as (
		select 'create table "' || EXA_SCHEMA_NAME || '"."' || EXA_TABLE_NAME || '" ( 
' || cols || '
)
;' as tbls from 
(select EXA_SCHEMA_NAME, EXA_TABLE_NAME, 
		group_concat( 
		case 
			when data_type in ('CHAR', 'NCHAR') then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'char(' || char_length || ')'
			when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(' || char_length || ')'
			when data_type = 'CLOB' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(2000000)'
			when data_type = 'XMLTYPE' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(2000000)'
			when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'decimal(' || data_precision || ',' || data_scale || ')'
			when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'decimal(' || data_precision || ',' || data_scale || ')'
			when data_type = 'NUMBER' and (data_length is not null and data_precision is null and data_scale is not null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'integer' 
			when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'double precision'
			when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'double precision'
			when data_type = 'DATE' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'timestamp'
			when data_type like 'TIMESTAMP(%)%' or data_type like 'TIMESTAMP' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'timestamp'
			when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'timestamp' 
			when data_type = 'BOOLEAN' then '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'boolean'
			else '--UNSUPPORTED DATATYPE IN COLUMN ' || EXA_COLUMN_NAME || ' Oracle Datatype: ' || data_type  end
			
		|| case when identity_column='YES' then ' IDENTITY' end
		|| case when nullable='N' then ' NOT NULL' end
		
	order by column_id SEPARATOR ', 
') as cols 
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
			else '--UNSUPPORTED DATATYPE IN COLUMN ' || COLUMN_NAME || ' Oracle Datatype: ' || data_type  
		end
				
	order by column_id SEPARATOR ', 
	'
	)
|| 

') from jdbc at ]]..CONNECTION_NAME..[[ statement 
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
			else '--UNSUPPORTED DATATYPE IN COLUMN ' || column_name || ' Oracle Datatype: ' || data_type  
		end
			
	order by column_id SEPARATOR ', 
	'
	)

|| ' from ' ||  '"' || owner || '"."' || table_name || '"' || ''';'  as imp from ora_base group by owner, table_name, EXA_SCHEMA_NAME, EXA_TABLE_NAME
	)
select '-- session parameter values are being taken from Oracle systemwide database_parameters and converted. However these should be confirmed before use.'
union all
select '-- Oracle DB''s NLS_CHARACTERSET is set to : ' || "VALUE" from nls_format where "PARAMETER"='NLS_CHARACTERSET'
union all
select '-- ALTER SESSION SET NLS_DATE_LANGUAGE=''' || "VALUE" || ''';' from nls_format where "PARAMETER"='NLS_DATE_LANGUAGE'
union all
select '-- ALTER SESSION SET NLS_DATE_FORMAT=''' || replace("VALUE",'R','Y') || ''';' from nls_format where "PARAMETER"='NLS_DATE_FORMAT'
union all
select '-- ALTER SESSION SET NLS_TIMESTAMP_FORMAT=''' || replace(regexp_replace("VALUE",'XF+','.FF6'),'R','Y') || ''';' from nls_format where "PARAMETER"='NLS_TIMESTAMP_FORMAT'
union all
select * from cr_schema
union all
select * from cr_tables
union all
select * from cr_import_stmts]],{c=CONNECTION_NAME, s=SCHEMA_FILTER, t=TABLE_FILTER})
if not success then error(res.error_message) end

return(res)
/

CREATE CONNECTION MY_ORACLE  --Install JDBC driver first via EXAoperation, see https://www.exasol.com/support/browse/SOL-179
	TO 'jdbc:oracle:thin:@//192.168.99.100:1521/xe'
	USER 'system'
	IDENTIFIED BY '********';


EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('MY_ORACLE_12C', '%APEX%','%',true);

EXECUTE SCRIPT database_migration.ORACLE_TO_EXASOL('MY_ORACLE_12C', '%HR%','%',true);