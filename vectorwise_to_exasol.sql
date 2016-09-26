open schema load_metadata;

/* This script will generate create table and create import statements to load all needed data from a Vectorwise database. 
Automatic datatype conversion is applied whenever needed. Feel free to adjust it. */

-- in EXASOL use JDBC driver for Java 7 i.e. 4.1.8 (install via EXAOperation -> Software -> JDBC Drivers)
-- http://esd.actian.com/product/drivers/JDBC/java
-- JDBC driver class: com.ingres.jdbc.IngresDriver
-- The connection string follows this form:
-- "jdbc:ingres://${HOSTNAME}:${PORT}/${DB};UID=${USERNAME};PWD=${PASSWORD}"
-- Port is the Data Access Server (DAS) Port (in my case 17031 - Actian Vector AP 3.5.1) or the replacement (AP7)
-- see in Actian Director Server -> Management -> Data Access Servers -> (default) -> ... [right click, "Properties"]; then "Protocols"

create or replace script load_metadata.LOAD_FROM_VECTOR(
CONNECTION_NAME --name of the database connection inside exasol -> e.g. vector_conn
,IDENTIFIER_CASE_INSENSITIVE -- if true then all is converted to uppercase in EXASOL, if false identifiers are double-qouted (")
,TABLE_FILTER --filter for the tables to generate and load -> '%' to load all
) RETURNS TABLE
AS
exa_upper_begin='"'
exa_upper_end='"'
if IDENTIFIER_CASE_INSENSITIVE == true then
	exa_upper_begin=''
	exa_upper_end=''
end
suc, res = pquery([[
with tbl_cols as (
select * from (import from jdbc at ]]..CONNECTION_NAME..[[ statement 'select t.table_name, column_name, column_datatype, column_length, column_scale, column_nulls, column_defaults, column_sequence -- getting all user tbls
from iitables t left join iicolumns c on c.table_name = t.table_name
where t.table_type = ''T'' and system_use = ''U'' and t.table_name not like ''ii%'' and t.table_name like '']]..TABLE_FILTER..[['' order by table_name, column_sequence asc')
),
tbl_ddl as (select 'create table ]]..exa_upper_begin..[[' || rtrim("table_name") || ']]..exa_upper_end..[[( '|| group_concat(']]..exa_upper_begin..[[' || rtrim("column_name") || ']]..exa_upper_end..[[ ' || 
	case 
		when rtrim("column_datatype") = 'INTEGER' then 'DECIMAL(' || "column_length" || ',' || "column_scale" || ')'
		when rtrim("column_datatype") = 'DECIMAL' then 'DECIMAL(' || "column_length" || ',' || "column_scale" || ')'
		when rtrim("column_datatype") = 'CHAR' then 'CHAR(' || "column_length" || ')' -- VW: fixed byte length
		when rtrim("column_datatype") = 'NCHAR' then 'CHAR(' || "column_length" || ')'-- VW: fixed character length
		when rtrim("column_datatype") = 'VARCHAR' then 'VARCHAR(' || "column_length" || ')'
		when rtrim("column_datatype") = 'NVARCHAR' then 'VARCHAR(' || "column_length" || ')'
		when rtrim("column_datatype") = 'FLOAT' then 'DOUBLE PRECISION'
		when rtrim("column_datatype") = 'MONEY' then 'DECIMAL(14,2)'
		when rtrim("column_datatype") = 'IPV4' then 'VARCHAR(16)'
		when rtrim("column_datatype") = 'IPV6' then 'VARCHAR(40)'
		when rtrim("column_datatype") = 'MONEY' then 'DECIMAL(14,2)'
		when rtrim("column_datatype") = 'INTEGER' then  case when "column_length" > 36 then 'DOUBLE PRECISION' else 'DECIMAL(' || "column_length" || ',' || "column_scale" || ')' end
		when rtrim("column_datatype") = 'ANSIDATE' then 'DATE'
		when rtrim("column_datatype") = 'TIMESTAMP WITHOUT TIME ZONE' then 'TIMESTAMP'
		when rtrim("column_datatype") = 'TIMESTAMP WITH LOCAL TIME ZONE' then 'TIMESTAMP WITH LOCAL TIME ZONE'
		when rtrim("column_datatype") = 'TEXT' then 'VARCHAR( 2000000 )'
		when rtrim("column_datatype") = 'BOOLEAN' then 'BOOLEAN'
    -- ### fallback for unknown types ###
		else '/*UNKNOWN_DATATYPE:' || "column_datatype" || '*/ varchar(2000000)' 
	end order by "column_sequence")
 || ' ); ' 
	as stmts from tbl_cols group by "table_name"),

tbl_imp as (select 'IMPORT INTO ]]..exa_upper_begin..[[' || rtrim("table_name") || ']]..exa_upper_end..[[(' || group_concat(']]..exa_upper_begin..[['|| rtrim("column_name") || ']]..exa_upper_end..[[' order by "column_sequence" ) || ') '  ||' FROM jdbc AT vector_conn STATEMENT ''select ' || group_concat(
	case 
		when rtrim("column_datatype") = 'IPV4' then 'cast('|| rtrim("column_name") || ' as varchar(16)) as ]]..exa_upper_begin..[[' || rtrim("column_name") || ']]..exa_upper_end..[['
		when rtrim("column_datatype") = 'IPV6' then 'cast('|| rtrim("column_name") || ' as varchar(40)) as ]]..exa_upper_begin..[[' || rtrim("column_name") || ']]..exa_upper_end..[['
		when rtrim("column_datatype") = 'MONEY' then 'cast('|| rtrim("column_name") || ' as decimal(14,2)) as ]]..exa_upper_begin..[[' || rtrim("column_name") || ']]..exa_upper_end..[['
		else ']]..exa_upper_begin..[[' || rtrim("column_name") || ']]..exa_upper_end..[['
	end
 order by "column_sequence"
) -- group_concat bracket
 || ' from ' || rtrim("table_name") || ''';' as s from tbl_cols group by "table_name")

select * from tbl_ddl
union all
select * from tbl_imp
]],{})

if not suc then
  error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
end

return(res)
/

execute script load_metadata.LOAD_FROM_VECTOR('vector_conn' --name of your database connection
,true
,'%' -- table filter --> '%' to load all tables (
);

ALTER CONNECTION VECTOR_CONN
	TO 'jdbc:ingres://192.168.137.4:AP7/sample'
	USER 'administrator'
	IDENTIFIED BY '********';

