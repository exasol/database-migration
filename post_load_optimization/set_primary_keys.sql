-- connection_type:						Type of connection, e.g. 'JDBC' or 'ORA'
-- connection_name:						Name of connection, e.g. 'MY_JDBC_MYSQL_CONNECTION'
-- connection_database_type:			                Type of database from which the keys should be migrated, currently 'ORACLE', 'MYSQL', 'SQLSERVER', 'POSTGRES' and 'EXASOL' are supported
-- schema_filter:						Filter for the schemas, e.g. '%' to take all schemas, 'my_schema' to load only primary_keys from this schema
-- table_filter:						Filter for the tables matching schema_filter, e.g. '%' to take all tables, 'my_table' to only load keys for this table
-- constraint_status:					Could be 'ENABLE' or 'DISABLE' and specifies whether the generated keys should be enabled or disabled
-- flag_identifier_case_insensitive: 	True if identifiers should be stored case-insensitiv (will be stored upper_case)
--/
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

function getPrimaryKeyColumnsFromExasol(connection_type, connection_name, schema_name, table_name)
	res = query([[select SCHEMA_NAME, TABLE_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', '), CONSTRAINT_NAME
	from(import from ::ct at ::cn statement '
		select constraint_schema as schema_name, constraint_table as table_name, column_name, constraint_name 
		from EXA_ALL_CONSTRAINT_COLUMNS
		where constraint_type = ''PRIMARY KEY''
		AND constraint_schema LIKE '']]..schema_name..[['' -- Schema Filter
		AND constraint_table LIKE '']] .. table_name..[['' -- Table Filter
	')
	GROUP BY (CONSTRAINT_NAME, SCHEMA_NAME, TABLE_NAME);]],{ct=connection_type, cn = connection_name, sn = schema_name})
	return res
end

function getPrimaryKeyColumnsFromMySql(connection_type, connection_name, schema_name, table_name)
	-- get columns for primary keys from mysql
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

function getPrimaryKeyColumnsFromPostgres(connection_type, connection_name, schema_name, table_name)
	-- get columns for primary keys from postgres
	-- owner, table_name, column_name
	res = query([[select TABLE_SCHEMA, TABLE_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', '), CONSTRAINT_NAME
	from(import from ::ct at ::cn statement '
		SELECT t.table_schema AS "TABLE_SCHEMA", t.table_name AS "TABLE_NAME", kcu.column_name AS "COLUMN_NAME", kcu.constraint_name AS "CONSTRAINT_NAME"
		FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                ON tc.table_catalog = t.table_catalog
                AND tc.table_schema = t.table_schema
                AND tc.table_name = t.table_name
                AND tc.constraint_type = ''PRIMARY KEY''
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON kcu.table_catalog = tc.table_catalog
                AND kcu.table_schema = tc.table_schema
                AND kcu.table_name = tc.table_name
                AND kcu.constraint_name = tc.constraint_name
		WHERE   t.table_schema like '']]..schema_name..[[''
		AND t.table_name like '']] .. table_name..[[''
	')
	GROUP BY (CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME);]], {ct=connection_type, cn = connection_name})
	return res
    
end

function getPrimaryKeyColumnsFromSqlserver(connection_type, connection_name, schema_name, table_name)
	-- get columns for primary keys from sqlserver
	-- owner, table_name, column_name
	res = query([[select SCHEMA_NAME, TABLE_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', '), CONSTRAINT_NAME
	from(import from ::ct at ::cn statement  '
                SELECT TC.table_schema as SCHEMA_NAME, KU.table_name as TABLE_NAME,column_name as COLUMN_NAME, TC.constraint_name as CONSTRAINT_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                INNER JOIN
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                          ON TC.CONSTRAINT_TYPE = ''PRIMARY KEY''
                             AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
                             AND TC.table_schema like '']]..schema_name..[['' -- Schema Filter
                             AND KU.table_name like '']] .. table_name..[['' -- Table Filter
                ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION'
        )
	GROUP BY (CONSTRAINT_NAME, SCHEMA_NAME, TABLE_NAME);]], {ct=connection_type, cn = connection_name, sn = schema_name})
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

-- query without group by here
function getNotNullColumnsFromExasol(connection_type, connection_name, schema_name, table_name)
	res = query([[select SCHEMA_NAME, TABLE_NAME, COLUMN_NAME
	from(import from ::ct at ::cn statement '
		select constraint_schema as schema_name, constraint_table as table_name, column_name 
		from EXA_ALL_CONSTRAINT_COLUMNS
		where constraint_type = ''NOT NULL''
		AND constraint_schema LIKE '']]..schema_name..[['' -- Schema Filter
		AND constraint_table LIKE '']] .. table_name..[['' -- Table Filter
	')
	;]],{ct=connection_type, cn = connection_name, sn = schema_name})
	return res
end

-- sets not null constraint for given column
-- if primary key should consist of multiple columns, column_name must be 'col_a, col_b, col_c'
-- constraint_status must be 'ENABLE' or 'DISABLE'
function setNotNull(schema_name, table_name, column_name, constraint_status)

	quoted_column_names = quoteCommaSeperatedString(column_name)
	output(quoted_column_names)

	succ, res = pquery([[
	ALTER TABLE ::sn.::tn MODIFY COLUMN ]]..quoted_column_names..[[ NOT NULL ]]..constraint_status..[[;
	]],{sn=quote(schema_name), tn=quote(table_name)})
	if (not succ) then
		output(res.statement_text)
		return false, 'Error not null: '.. res.error_message
	end
	return true, 'SET as NOT NULL'
end


------------------------------------------------------------------------------------------------------
-- get information about foreign keys from other db
-- returns table consisting of: constraint_name, schema_name, table_name, column_name, ref_schema_name, ref_table_name
function getForeignKeyInformationFromForeignDb(connection_type, connection_name, connection_database_type, schema_name, table_name)
	foreignDbStatement = [[]]
	if(connection_database_type == 'MYSQL') then
		foreignDbStatement = [[
		SELECT cu.constraint_name, cu.TABLE_SCHEMA, cu.TABLE_NAME, GROUP_CONCAT(c.COLUMN_NAME SEPARATOR '', '') as column_name, cu.REFERENCED_TABLE_SCHEMA, cu.REFERENCED_TABLE_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
		JOIN information_schema.columns c ON (c.table_schema = cu.table_schema and c.table_name = cu.table_name and c.column_name = cu.column_name) 
		WHERE cu.table_schema like '']]..schema_name..[[''
		AND cu.table_name like '']]..table_name..[[''
        AND cu.REFERENCED_TABLE_NAME is not null -- to prevent the evalauation of unique constraints
		GROUP BY cu.Constraint_name, cu.table_schema, cu.table_name, cu.REFERENCED_TABLE_SCHEMA, cu.REFERENCED_TABLE_NAME
		HAVING max(c.column_key) = ''MUL'';
		]]
		
		-- could not be unified for all connectoin_database_types because the select a as b does not work if already applied in the foreignDbStatement for Mysql
		succ, res = pquery([[select *
		from(import from ::ct at ::cn statement ']]..foreignDbStatement..[[')
		;]], {ct=connection_type, cn = connection_name, sn = schema_name})

		
	else
		if (connection_database_type == 'ORACLE') then
			foreignDbStatement = [[
			SELECT c.constraint_name, c.owner as schema_name, c.table_name as table_name, c2.owner as referenced_schema, c2.table_name as referenced_table, cols.column_name as column_name
			FROM all_constraints c 
			JOIN all_constraints c2 ON (c.r_constraint_name = c2.constraint_name) 
			JOIN all_cons_columns cols ON (cols.constraint_name = c.constraint_name)
			WHERE c.OWNER LIKE '']]..schema_name..[['' -- Schema Filter
			AND c.table_name LIKE '']] .. table_name..[['' -- Table Filter
			AND c.constraint_TYPE = ''R''
			]]
	        elseif(connection_database_type == 'SQLSERVER') then
	               foreignDbStatement = [[
                        SELECT  
                            obj.name AS [CONSTRAINT_NAME],
                            sch.name AS [SCHEMA_NAME],
                            tab1.name AS [TABLE_NAME],
                            sch2.name AS [REFERENCED_SCHEMA],
                            tab2.name AS [REFERENCED_TABLE],
                            col1.name AS [COLUMN_NAME]
                        FROM sys.foreign_key_columns fkc
                        INNER JOIN sys.objects obj
                            ON obj.object_id = fkc.constraint_object_id
                        INNER JOIN sys.tables tab1
                            ON tab1.object_id = fkc.parent_object_id
                        INNER JOIN sys.schemas sch
                            ON tab1.schema_id = sch.schema_id
                        INNER JOIN sys.columns col1
                            ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
                        INNER JOIN sys.tables tab2
                            ON tab2.object_id = fkc.referenced_object_id
                        INNER JOIN sys.schemas sch2
                            ON tab2.schema_id = sch2.schema_id
                        WHERE sch.name like '']]..schema_name..[['' -- Schema Filter
                        AND tab1.name like '']] .. table_name..[['' -- Table Filter
                        ORDER BY obj.name, fkc.referenced_column_id
			]]
		elseif(connection_database_type == 'POSTGRES') then
	               foreignDbStatement = [[
                	SELECT 
    			 tc.constraint_name AS "CONSTRAINT_NAME", 
    		 	 tc.table_schema AS "SCHEMA_NAME",
    	   	 	 tc.table_name AS "TABLE_NAME", 
    			 ccu.table_schema AS "REFERENCED_SCHEMA",
    			 ccu.table_name AS "REFERENCED_TABLE",
    		 	 kcu.column_name AS "COLUMN_NAME" 
			 FROM 
    			 information_schema.table_constraints AS tc 
    			 JOIN information_schema.key_column_usage AS kcu
      			 ON tc.constraint_name = kcu.constraint_name
      			 AND tc.table_schema = kcu.table_schema
    			 JOIN information_schema.constraint_column_usage AS ccu
      			 ON ccu.constraint_name = tc.constraint_name
      			 AND ccu.table_schema = tc.table_schema
			 WHERE tc.constraint_type = ''FOREIGN KEY'' 
			 AND tc.table_schema like '']]..schema_name..[['' -- Schema Filter
		 	 AND tc.table_name like '']] .. table_name..[['' -- Table Filter      
			 ORDER BY tc.constraint_name, ccu.column_name
			]]
		elseif(connection_database_type == 'EXASOL') then
			foreignDbStatement = [[
			SELECT constraint_name, constraint_schema as schema_name, constraint_table as table_name, referenced_schema, referenced_table, column_name
			FROM EXA_ALL_CONSTRAINT_COLUMNS
			WHERE constraint_type = ''FOREIGN KEY''
			AND constraint_schema LIKE '']]..schema_name..[['' -- Schema Filter
			AND constraint_table LIKE '']] .. table_name..[['' -- Table Filter
 			]]
		else
			error('Unknown connection database type: '..connection_database_type)
		end
		
		succ, res = pquery([[select constraint_name, schema_name, table_name, GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ') , max(referenced_schema), max(referenced_table)
			from(import from ::ct at ::cn statement ']]..foreignDbStatement..[[')
			GROUP BY (CONSTRAINT_NAME, SCHEMA_NAME, TABLE_NAME)
			;]], {ct=connection_type, cn = connection_name, sn = schema_name})

	end

	if not succ then
		error('Error in getForeignKeyInformationFromForeignDb: \n' .. res.error_message .. '\nStatement was: '.. res.statement_text)
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
			return false, 'Error foreign key: ' .. res.error_message .. '\n'.. res.statement_text
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
if(connection_database_type ~= 'ORACLE' and connection_database_type ~= 'MYSQL' and connection_database_type ~= 'EXASOL' and connection_database_type ~= 'SQLSERVER' and connection_database_type ~= 'POSTGRES') then
	error([[Please specify a proper connection_database_type. Could be 'ORACLE', 'MYSQL', 'SQLSERVER', 'POSTGRES' or 'EXASOL']])
end

-- get and set primary keys
if(connection_database_type == 'ORACLE') then
	prim_cols = getPrimaryKeyColumnsFromOracle(connection_type, connection_name, schema_filter, table_filter)
elseif (connection_database_type == 'EXASOL') then
	prim_cols = getPrimaryKeyColumnsFromExasol(connection_type, connection_name, schema_filter, table_filter)
elseif (connection_database_type == 'SQLSERVER') then
	prim_cols = getPrimaryKeyColumnsFromSqlserver(connection_type, connection_name, schema_filter, table_filter)
elseif (connection_database_type == 'POSTGRES') then
	prim_cols = getPrimaryKeyColumnsFromPostgres(connection_type, connection_name, schema_filter, table_filter)
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

-- get and set not null constraints
if (connection_database_type == 'EXASOL') then

	not_null_cols = getNotNullColumnsFromExasol(connection_type, connection_name, schema_filter, table_filter)

	for i=1,#not_null_cols do
	
		if(flag_identifier_case_insensitive) then
			nn_schema_name		= string.upper(not_null_cols[i][1])
			nn_table_name		= string.upper(not_null_cols[i][2])
			nn_column_name		= string.upper(not_null_cols[i][3])

		else
			nn_schema_name		= not_null_cols[i][1]
			nn_table_name		= not_null_cols[i][2]
			nn_column_name		= not_null_cols[i][3]
		end
	
		result_success, result_text = setNotNull(nn_schema_name, nn_table_name, nn_column_name, constraint_status)
		result_table[#result_table+1] = {nn_schema_name, nn_table_name, nn_column_name, result_success, result_text}
	end
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


exit(result_table, "schema_name varchar(200), table_name varchar(200), column_name varchar(200), success boolean, actions varchar(20000)")
/


-- TODO: Also generate not null constraints

-- Examples of usage
execute script DATABASE_MIGRATION.SET_PRIMARY_AND_FOREIGN_KEYS(
'JDBC',				-- connection_type:						Type of connection, e.g. 'JDBC' or 'ORA'
'JDBC_SQLSERVER',		-- connection_name:						Name of connection
'SQLSERVER',			-- connection_database_type:			                Type of database from which the keys should be migrated, currently 'ORACLE', 'MYSQL', 'SQLSERVER', 'POSTGRES' and 'EXASOL' are supported
'MY_SCHEMA',   		        -- schema_filter:						Filter for the schemas, e.g. '%' to take all schemas, 'my_schema' to load only primary_keys from this schema
'%',				-- table_filter:						Filter for the tables matching schema_filter, e.g. '%' to take all tables, 'my_table' to only load keys for this table
'DISABLE',		        -- constraint_status:					        Could be 'ENABLE' or 'DISABLE' and specifies whether the generated keys should be enabled or disabled
'false'		                -- flag_identifier_case_insensitive: 	                        True if identifiers should be stored case-insensitiv (will be stored upper_case)
);

-- Second example
execute script database_migration.set_primary_and_foreign_keys('ORA', 'MY_CONN','ORACLE', 'SCOTT', 'MY_%', 'DISABLE', true);