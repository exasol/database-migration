-- connection_type:						Type of connection, e.g. 'JDBC' or 'ORA'
-- connection_name:						Name of connection, e.g. 'MY_JDBC_MYSQL_CONNECTION'
-- connection_database_type:			Type of database from which the keys should be migrated, currently 'ORACLE' and 'MYSQL' are supported
-- schema_filter:						Filter for the schemas, e.g. '%' to take all schemas, 'my_schema' to load only primary_keys from this schema
-- table_filter:						Filter for the tables matching schema_filter, e.g. '%' to take all tables, 'my_table' to only load keys for this table
-- constraint_status:					Could be 'ENABLE' or 'DISABLE' and specifies whether the generated keys should be enabled or disabled
-- flag_identifier_case_insensitive: 	True if identifiers should be stored case-insensitiv (will be stored upper_case)
create or replace script database_migration.set_primary_and_foreign_keys(connection_type, connection_name,connection_database_type, schema_filter, table_filter, constraint_status, flag_identifier_case_insensitive) RETURNS TABLE
 as
------------------------------------------------------------------------------------------------------
-- returns colums for primary keys, if primary key constists of multiple colums they are returned as comma separated list
-- returns table consisting of: schema_name, table_name, column_name, constraint_name
function getPrimaryKeyColumnsFromOracle(connection_type, connection_name, schema_name, table_name)
	-- get columns for primary keys from oracle
	-- owner, table_name, column_name
	res = query([[select SCHEMA_NAME, TABLE_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', '), CONSTRAINT_NAME
	from(import from ::ct at ::cn statement '
		SELECT cons.owner as SCHEMA_NAME, cols.table_name, cols.column_name, cons.constraint_name
		FROM all_constraints cons, all_cons_columns cols
		WHERE cons.owner = cols.owner
		AND cons.OWNER LIKE '']]..schema_name..[['' -- Schema Filter
		AND cols.table_name LIKE '']] .. table_name..[['' -- Table Filter
		AND cons.constraint_type = ''P''
		AND cons.constraint_name = cols.constraint_name
	')
	GROUP BY (CONSTRAINT_NAME, SCHEMA_NAME, TABLE_NAME);]], {ct=connection_type, cn = connection_name, sn = schema_name})
	return res
end

function getPrimaryKeyColumnsFromMySql(connection_type, connection_name, schema_name, table_name)
	-- get columns for primary keys from oracle
	-- owner, table_name, column_name
	res = query([[select TABLE_SCHEMA, TABLE_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', '), CONSTRAINT_NAME
	from(import from ::ct at ::cn statement '
		SELECT cu.TABLE_SCHEMA, cu.TABLE_NAME, cu.COLUMN_NAME, cu.CONSTRAINT_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
		JOIN information_schema.columns c ON (c.table_schema = cu.table_schema and c.table_name = cu.table_name and c.column_name = cu.column_name) 
		WHERE c.column_key = ''PRI''
		AND cu.table_schema like '']]..schema_name..[[''
		AND cu.table_name like '']] .. table_name..[[''
	')
	GROUP BY (CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME);]], {ct=connection_type, cn = connection_name})
	return res
end

------------------------------------------------------------------------------------------------------
-- sets primary key for given column
-- if primary key should consist of multiple columns, column_name must be 'col_a, col_b, col_c'
-- constraint_status must be 'ENABLE' or 'DISABLE'
function setPrimaryKey(schema_name, table_name, column_name, constraint_name, constraint_status)

	quoted_column_names = quoteCommaSeperatedString(column_name)
	output(quoted_column_names)

	succ, res = pquery([[
	ALTER TABLE ::sn.::tn ADD CONSTRAINT ::conname PRIMARY KEY (]]..quoted_column_names..[[) ]]..constraint_status..[[;
	]],{sn=quote(schema_name), tn=quote(table_name), conname= quote(constraint_name)})
	if (not succ) then
		output(res.statement_text)
		return false, 'Error primary key: '.. res.error_message
	end
	return true, 'SET as PRIMARY KEY'
end

------------------------------------------------------------------------------------------------------
-- get information about foreign keys from oracle
-- returns table consisting of: constraint_name, schema_name, table_name, column_name, ref_schema_name, ref_table_name
function getForeignKeyInformationFromForeignDb(connection_type, connection_name, connection_database_type, schema_name, table_name)
	foreignDbStatement = [[]]
	if(connection_database_type == 'ORACLE') then
		foreignDbStatement = [[
		SELECT c.constraint_name, c.owner as schema_name, c.table_name as table_name, c2.owner as referenced_schema, c2.table_name as referenced_table, cols.column_name as column_name
		FROM all_constraints c 
		JOIN all_constraints c2 ON (c.r_constraint_name = c2.constraint_name) 
		JOIN all_cons_columns cols ON (cols.constraint_name = c.constraint_name)
		WHERE c.OWNER LIKE '']]..schema_name..[['' -- Schema Filter
		AND c.table_name LIKE '']] .. table_name..[['' -- Table Filter
		AND c.constraint_TYPE = ''R''
		]]

		succ, res = pquery([[select constraint_name, schema_name, table_name, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ') , max(referenced_schema), max(referenced_table)
		from(import from ::ct at ::cn statement ']]..foreignDbStatement..[[')
		GROUP BY (CONSTRAINT_NAME, SCHEMA_NAME, TABLE_NAME)
		;]], {ct=connection_type, cn = connection_name, sn = schema_name})

	elseif(connection_database_type == 'MYSQL') then
		foreignDbStatement = [[
		SELECT cu.constraint_name, cu.TABLE_SCHEMA, cu.TABLE_NAME, GROUP_CONCAT(c.COLUMN_NAME SEPARATOR '', '') as column_name, cu.REFERENCED_TABLE_SCHEMA, cu.REFERENCED_TABLE_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
		JOIN information_schema.columns c ON (c.table_schema = cu.table_schema and c.table_name = cu.table_name and c.column_name = cu.column_name) 
		WHERE cu.table_schema like '']]..schema_name..[[''
		AND cu.table_name like '']]..table_name..[[''
		GROUP BY cu.Constraint_name, cu.table_schema, cu.table_name, cu.REFERENCED_TABLE_SCHEMA, cu.REFERENCED_TABLE_NAME
		HAVING max(c.column_key) = ''MUL'';
		]]
		
		-- could not be unified for all connectoin_database_types because the select a as b does not work if already applied in the foreignDbStatement for Mysql
		succ, res = pquery([[select *
		from(import from ::ct at ::cn statement ']]..foreignDbStatement..[[')
		;]], {ct=connection_type, cn = connection_name, sn = schema_name})
	else
		error('Unknown connection database type: '..connection_database_type)
	end

	if not succ then
		error('Error in getForeignKeyInformationFromForeignDb: ' .. res.error_message)
	end
	return(res)
end

------------------------------------------------------------------------------------------------------
-- sets foreign key for given column
-- if key should consist of multiple columns, column_name must be 'col_a, col_b, col_c'
-- constraint_status must be 'ENABLE' or 'DISABLE'
function setForeignKey(schema_name, table_name, column_name, schema_ref_name, table_ref_name, foreign_key_name, constraint_status)
	
	quoted_column_names = quoteCommaSeperatedString(column_name)
	succ, res = pquery([[
	ALTER TABLE ::sn.::tn
	ADD	CONSTRAINT ::fkn FOREIGN KEY(]]..quoted_column_names..[[) REFERENCES ::srn.::trn ]]..constraint_status..[[;
	]],{sn=quote(schema_name), tn=quote(table_name), fkn=quote(foreign_key_name), srn=quote(schema_ref_name), trn=quote(table_ref_name)})

	if(not succ) then
			return false, 'Error foreign key: ' .. res.error_message
	end
	return true, 'SET as FOREIGN KEY to '.. schema_ref_name..'.'..table_ref_name
end

------------------------------------------------------------------------------------------------------
-- Helper function to quote stuff, examples:
-- a,b --> "a","b"
-- "a","b" --> "a","b"
-- "a,b" --> "a","b"
function quoteCommaSeperatedString(to_quote)
	-- first: remove all blanks to be sure that everything is quoted in a correct way, replace blank --> nothing
	quoted = string.gsub(to_quote, ' ', '')
	-- then: quote everything, replace , --> ","
	quoted = '\"' .. string.gsub(quoted, ',', '\",\"') .. '\"'
	-- if it was already quoted before we have two quotes everywhere now --> replace two quotes by one
	quoted = string.gsub(quoted,'\"\"', '\"')
	return quoted
end
------------------------------------------------------------------------------------------------------
-----------------------------END OF FUNCTIONS, BEGINNING OF ACTUAL SCRIPT-----------------------------
result_table = {}

if(constraint_status ~= 'ENABLE' and constraint_status ~= 'DISABLE') then
	constraint_status= 'DISABLE'
end

connection_database_type = string.upper(connection_database_type)
if(connection_database_type ~= 'ORACLE' and connection_database_type ~= 'MYSQL') then
	error([[Please specify a proper connection_database_type. Could be 'ORACLE' or 'MYSQL']])
end

-- get and set primary keys
if(connection_database_type == 'ORACLE') then
	prim_cols = getPrimaryKeyColumnsFromOracle(connection_type, connection_name, schema_filter, table_filter)
else
	prim_cols = getPrimaryKeyColumnsFromMySql(connection_type, connection_name, schema_filter, table_filter)
end

for i=1,#prim_cols do

	if(flag_identifier_case_insensitive) then
		pk_schema_name		= string.upper(prim_cols[i][1])
		pk_table_name		= string.upper(prim_cols[i][2])
		pk_column_name		= string.upper(prim_cols[i][3])
		pk_constraint_name	= string.upper(prim_cols[i][4])
		fk_table_ref_name	= string.upper(prim_cols[i][1])
	else
		pk_schema_name		= prim_cols[i][1]
		pk_table_name		= prim_cols[i][2]
		pk_column_name		= prim_cols[i][3]
		pk_constraint_name	= prim_cols[i][4]
		fk_table_ref_name	= prim_cols[i][1]
	end

	result_success, result_text = setPrimaryKey(pk_schema_name, pk_table_name, pk_column_name, pk_constraint_name, constraint_status)
	result_table[#result_table+1] = {pk_schema_name, pk_table_name, pk_column_name, result_success, result_text}
end

-- get and set foreign keys

-- see text above function "getForeignKeyInformationFromForeignDb" for details about return values
fks = getForeignKeyInformationFromForeignDb(connection_type, connection_name, connection_database_type, schema_filter, table_filter)

for i=1,#fks do

	if(flag_identifier_case_insensitive) then
		fk_name				= fks[i][1]
		fk_schema_name 		= string.upper(fks[i][2])
		fk_table_name		= string.upper(fks[i][3])
		fk_column_name		= string.upper(fks[i][4])
		fk_schema_ref_name 	= string.upper(fks[i][5])
		fk_table_ref_name	= string.upper(fks[i][6])
	else
		fk_name				= fks[i][1]
		fk_schema_name 		= fks[i][2]
		fk_table_name		= fks[i][3]
		fk_column_name		= fks[i][4]
		fk_schema_ref_name 	= fks[i][5]
		fk_table_ref_name	= fks[i][6]
	end


	result_success, result_text = setForeignKey(fk_schema_name, fk_table_name, fk_column_name, fk_schema_ref_name, fk_table_ref_name, fk_name, constraint_status)
	result_table[#result_table+1] = {fk_schema_name, fk_table_name, fk_column_name, result_success, result_text}
end


exit(result_table, "schema_name char(200), table_name char(200), column_name char(200), success boolean, actions char(200)")
/


-- TODO: Also generate not null constraints

-- Examples of usage
execute script database_migration.set_primary_and_foregin_keys(
'JDBC',				-- connection_type:						Type of connection, e.g. 'JDBC' or 'ORA'
'JDBC_MYSQL',		-- connection_name:						Name of connection
'MYSQL',			-- connection_database_type:			Type of database from which the keys should be migrated, currently 'ORACLE' and 'MYSQL' are supported
'MY_SCHEMA',		-- schema_filter:						Filter for the schemas, e.g. '%' to take all schemas, 'my_schema' to load only primary_keys from this schema
'%',				-- table_filter:						Filter for the tables matching schema_filter, e.g. '%' to take all tables, 'my_table' to only load keys for this table
'DISABLE',		-- constraint_status:					Could be 'ENABLE' or 'DISABLE' and specifies whether the generated keys should be enabled or disabled
'false'		-- flag_identifier_case_insensitive: 	True if identifiers should be stored case-insensitiv (will be stored upper_case)
);

-- Second example
execute script database_migration.set_primary_and_foreign_keys('ORA', 'MY_CONN','ORACLE', 'SCOTT', 'MY_%', 'DISABLE', true);