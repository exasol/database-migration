/**
** Please be aware before proceeding you must have the desired schema open for the script to create the star schema.
** The first parameter required is the location of the flat table.
** The second is the desired fact table name.
** Third requires a boolean value to whether or not allow the script to create the tables for them.
** Fourth also requires a boolean if you wish to transfer all the data over found in the flat table.
** The last set of parameters are arrays, the first array is a list of dimension tables the user would like to have implemented. The second array is the list of columns to be allocated into the dimensions.
** To allocate a column to a dimension the user must write the number referencing the location of the dimension name in the first array followed by a pipe then the column name.
** An example of an execute statement can be seen at the bottom.
**/
CREATE OR REPLACE LUA SCRIPT CONVERT_TO_STAR_SCHEMA(flat_table, given_fact_table_name, do_create_tables, transfer_data, ARRAY dimension_table_names, ARRAY col_with_index) RETURNS TABLE AS
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	local target_schema
	local raw_target_table
	local target_table
	local dimensions
	local factTable
	local factTableIndividualColumns
----------------------------------------------------------------------------------------------
	/**
	** This will break up into two sets of strings from the parameter flat_table, which will be broken into two parts which is later used in different segments in the code. 
	**
	**/
	if (string.match(flat_table, '%.')) then
		target_schema = string.sub(flat_table, 1, (string.find(flat_table, '%.')- 1))
		raw_target_table = string.sub(flat_table, (string.find(flat_table, '%.')+ 1))
		target_table = '"' .. raw_target_table .. '"'
		output('Schema specified: ' .. target_schema .. '.' .. target_table)
	else
		error('There is no schema specified!')
	end
----------------------------------------------------------------------------------------------
	/**
	** This entire section mainly involves printing lua tables to the console.
	** These functions were built for debugging purposes.
	**/
	function makeLine(num, symbol)
		theString = ''
		for i=0, num do
			theString = theString .. symbol
		end
		return theString
	end
	
	function printStr(content, title)
		local content = '|     ' .. content .. '     |'
		local spacToAdd = #content - #title
		output(makeLine(#content-1, '-'))
		output('|     ' .. title .. makeLine(spacToAdd - 8, ' ') .. '|')
		output(makeLine(#content-1, '-'))
		output(content)
		output(makeLine(#content-1, '-'))
		output(' ')
		output(' ')
	end
	
	function showTable(table_to_show, title)
		local spac = {}

		for i=1, #table_to_show do		   
			for q=1, #table_to_show[1] do
		
				if i == 1 then
					spac[#spac+1] = 0
				end
				
				local conv = tostring(table_to_show[i][q])
				
				if #conv > spac[q] then
					spac[q] = #conv
				end
			end
		end
		
		for a=1, #table_to_show do
			local line = 0
			output_string = ''
			
			for b=1, #table_to_show[a] do
			
				local conv = tostring(table_to_show[a][b])
				
				local theInput = ((spac[b]) - #conv) + 5
				output_string = output_string .. '|     ' .. tostring(table_to_show[a][b]) .. makeLine(theInput, ' ')
				
				if b == #table_to_show[a] then
					output_string = output_string .. '|'
				end
			end
			
			title = tostring(title)
			local spacToAdd = #output_string - #title
			
			if a == 1 then 
				output(makeLine(#output_string - 1, '-'))
				output('|     ' .. title .. makeLine(spacToAdd - 8, ' ') .. '|')
				output(makeLine(#output_string - 1, '-'))
				output(output_string)
			elseif a == #table_to_show then
				output(output_string)
				output(makeLine(#output_string - 1, '-'))
				output(' ')
				output(' ')

			else
				output(output_string)
			end
		end
	end
----------------------------------------------------------------------------------------------
	/**
	** This is a function used in multiple functions within the script.
	** The idea of the function is to find out if the string supplied in the first parameter exists in the lua table supplied in the second parameter.
	** A column must be selected in the table for the function to read each row. If nothing is found there false will be returned but if something is found then the poistion of the string in the table will be returned.
	**/
	function findRelation(string, tab, columnIndex)
		valid = false
		for a = 1, #tab do
			if ((string.match(string.lower(tab[a][columnIndex]), string.lower(string)))) and (#string > #tab[a][columnIndex]-1) then
				valid = a
			end
		end
		return valid
	end
----------------------------------------------------------------------------------------------
	/**
	** The very first stage of the script is to separate the indexes attached to the column names from the flat table specified by the user.
	** The function by the end will return a table with the index to the supplied dimension names from the user.
	**/
	function separateNumbersFromColumns()
		local localTable = {}
		
		if #col_with_index >= 1 then
			for i=1, #col_with_index do
				if (string.match(col_with_index[i], '|')) then
					local num = tonumber(string.sub(col_with_index[i], 1, string.find(col_with_index[i], '|')- 1))				
					local usr_column = string.sub(col_with_index[i], string.find(col_with_index[i], '|')+ 1)
					if num >= 0 then
						localTable[#localTable+1] = {num, usr_column}
					else
						error('Could not identify a number')
					end
				else
					error('Cannot identify the indexes from the columns given')
				end
			end
		else
			error('No columns names specified')
		end

		return localTable
	end
----------------------------------------------------------------------------------------------
	/**
	** The details of every column in the database can be found in the system tables and the idea of this function is to get those details which we will be needed later to generate the create statements in SQL.
	** When calling the function you will need the schema of the flat table, along with the table name.
	**/
	function getColumns(schema_name, table_name)
		nc_ok, columns_from_system = pquery([[
			WITH R1 AS (			
			SELECT 
				COLUMN_SCHEMA,
				COLUMN_TABLE,
				COLUMN_NAME,
				COLUMN_TYPE,
				COLUMN_TYPE_ID
			FROM 
				EXA_ALL_COLUMNS 
			WHERE 
				UPPER(COLUMN_SCHEMA) = :schema_filter AND
				UPPER(COLUMN_TABLE) = :table_filter
			)
			
			SELECT
				*
			FROM 
				R1 
			--WHERE
			--	COLUMN_TYPE_ID = 3 OR
			--	COLUMN_TYPE_ID = 8 OR
			--	COLUMN_TYPE_ID = 91 OR
			--	COLUMN_TYPE_ID = 93
		]], {schema_filter = string.upper(schema_name), table_filter = string.upper(table_name)})
		
		if (nc_ok) and (#columns_from_system ~= 0) then
			output('function getColumns() called')
		else
			error('Could not get columns from system tables.')
		end
		
		return columns_from_system
	end
----------------------------------------------------------------------------------------------
	/**
	** The returned tables of the previous two functions are the parameters this function requires.
	** The idea here is to use each index supplied by the user as a map to the dimension names the user created.
	** There is error handling handled here, where each column the user has specified is checked if it exists from the information supplied from the system tables.
	** 
	**/
	function checkGivenColNamesWithFlat(columnsFromSystem, columnsFromUser)
		local valid = false
		
		local counter = 0
		local mainTable = {}
		local errorTab = {}

		for i=1, #columnsFromUser do
			if findRelation(columnsFromUser[i][2], columnsFromSystem, 3) == false then
				errorTab[#errorTab+1] = string.upper(columnsFromUser[i][2])
			end
		end

		for a=1, #columnsFromSystem do
			rel = findRelation(columnsFromSystem[a][3], columnsFromUser, 2)
		
			if rel ~= false then
				mainTable[#mainTable+1] = {columnsFromUser[rel][1], '"' .. columnsFromSystem[a][3] .. '"', columnsFromSystem[a][4], columnsFromSystem[a][5], false }
			elseif rel == false then
				mainTable[#mainTable+1] = {given_fact_table_name, '"' .. columnsFromSystem[a][3] .. '"', columnsFromSystem[a][4], columnsFromSystem[a][5], true }
			end
		end

		if #errorTab ~= 0 then
			errorStr = 'Could not find columns: '
			for b = 1, #errorTab do
				errorStr = errorStr .. errorTab[b]
				if b ~= #errorTab then
					errorStr = errorStr .. ', '
				end
			end
			error(errorStr)
		end

		local mapToDim = {}
		if (#dimension_table_names >= 1) then
			for b=1, #mainTable do
				local num = 0
				
				if string.match(tostring(mainTable[b][1]), given_fact_table_name) then
				    mapToDim[#mapToDim+1] = {mainTable[b][1], mainTable[b][2], mainTable[b][3], mainTable[b][4], mainTable[b][5]}
				else
					num = mainTable[b][1]
					if dimension_table_names[num] == nil then
						error('Could not find dimension ' .. num .. ' in the array.')
					else
						mapToDim[#mapToDim+1] = {string.upper(dimension_table_names[num]), mainTable[b][2], mainTable[b][3], mainTable[b][4], mainTable[b][5]}
					end
				end
			end
		else
			error('There are no dimension tables specified')
		end
		
		return mapToDim
	end
---------------------------------------------------------------------------------------------
	/**
	** This function requires a lua table with the following specification: {DIMENSION NAME | COLUMN NAME | TYPE OF COLUMN | TYPE IDENTIFIER | ADD TO FACT TABLE OR NOT}
	** 
	** The next step is to join some of the columns together to form a string so we can begin constructing the create statement.
	** The column names will be placed in one line with their type in a single column in a lua table along with the dimension name they are assigned to in another column of the lua table.
	** Primary keys and foreign keys are genereated in this function to be later used for the create statements and insert statements.
	** There are 3 global varables which become initialised here:
	**      dimensions = 
	**              {DIMENSION NAME | ALL COLUMN NAMES WITH THEIR TYPE | PRIMARY KEY | FOREIGN KEY}
	** The fact table has 3 rows with each row structured differently:
	**      factTable =
	**              {COLUMN NAME WITH TYPE}
	**              {FLAT TABLE NAME WITH EACH COLUMN NAME}
	**              {COLUMN NAMES IN A SINGLE STRING}
	** The idea of the factTableIndividualColumns is to have each column of the fact table separated so they can be easily be accessed for the insert statement:
	**      factTableIndividualColumns = 
	**              {DIMENSION NAME | COLUMN NAME}
	** There is no return statement because each variable set is global so therefore does not need to be returned.
	**/
	function separeteDimensions(tab)
		dimensions = {}
		factTable = {}
		factTableIndividualColumns = {}
		
		for a = 1, #tab do
			if tab[a][5] == true then

				if factTable[1] == nil then
					factTable[1] = {tab[a][2] .. ' ' .. tab[a][3]}
					factTable[2] = {target_table .. '.' .. tab[a][2]}
					factTable[3] = {tab[a][2]}
					
					factTableIndividualColumns[#factTableIndividualColumns+1] = {tab[a][1], tab[a][2]}
				else
					factTable[1][1] = factTable[1][1] .. ', ' .. tab[a][2] .. ' ' .. tab[a][3]
					factTable[2][1] = factTable[2][1] .. ', ' .. target_table .. '.' .. tab[a][2]
					factTable[3][1] = factTable[3][1] .. ', ' .. tab[a][2]
					
					factTableIndividualColumns[#factTableIndividualColumns+1] = {tab[a][1], tab[a][2]}
				end

			elseif findRelation(tab[a][1], dimensions, 1) == false  then
				if tab[a][5] == false then
					dimensions[#dimensions+1] = {tab[a][1], tab[a][2] .. ' ' .. tab[a][3], string.upper(tab[a][1]) .. '_PK', string.upper(tab[a][1]) .. '_FK' }
				end

			elseif findRelation(tab[a][1], dimensions, 1) ~= false then
			
				num = findRelation(tab[a][1], dimensions, 1)
				dimensions[num][2] = dimensions[num][2] .. ', ' .. tab[a][2] .. ' ' .. tab[a][3]
				
			end
		end
		--return dimensions
	end
----------------------------------------------------------------------------------------------
	/**
	** This is when the create statements for the dimension tables begin to take form, using a lua table defined as dimensions.
	** The second column of this lua table contains the column names along with their identifiers in a single string.
	**/
	function makeDimCreateStatements()
		local dimCreateStatements = {}
		
		for a = 1, #dimensions do
			local str = 'CREATE OR REPLACE TABLE ' .. dimensions[a][1] .. ' (' .. dimensions[a][3] .. ' DECIMAL(10,0) IDENTITY, ' .. dimensions[a][2] .. ', PRIMARY KEY(' .. dimensions[a][3] ..'))'
			dimCreateStatements[#dimCreateStatements+1] = {str}
		end
		return dimCreateStatements
	end
----------------------------------------------------------------------------------------------
	/**
	** Using the lua table which has taken shape in the separateDimension() function.
	**
	**/
	function makeFactCreateStatements()
		local factCreateStatements = ''
		for a = #dimensions, 1, -1 do
			factTable[1][1] = dimensions[a][4] .. ' DECIMAL(10,0), ' .. factTable[1][1] .. ', FOREIGN KEY(' ..  dimensions[a][4] .. ') REFERENCES ' .. dimensions[a][1] ..'(' .. dimensions[a][3] .. ')'
		end
		
		factCreateStatements = 'CREATE OR REPLACE TABLE ' .. given_fact_table_name .. ' (' .. factTable[1][1] .. ')'
		
		return factCreateStatements
	end
----------------------------------------------------------------------------------------------
	/**
	**
	**
	**/
	function getDimensionDetails(finalTab, dimensionName)
		local focusedDimension = {}

		for a = 1, #finalTab do
			if finalTab[a][1] == dimensionName then
				focusedDimension[#focusedDimension+1] = {finalTab[a][2], finalTab[a][3], finalTab[a][4], finalTab[a][5]}
			end
		end
		return focusedDimension
	end
----------------------------------------------------------------------------------------------
	-- The contents of this variable is created in the function makeDimInsertStatements() and is used again in makeFactInsertStatements()
	local columns = {}
----------------------------------------------------------------------------------------------
	/**
	**
	**
	**/
	function makeDimInsertStatements(tab)
		--local columns = {}
		local insertStatement = {}
		
		for a = 1, #tab do
			if findRelation(tab[a][1], columns, 1) == false then

				if tab[a][5] == false then
					columns[#columns+1] = {tab[a][1], tab[a][2], '((' .. tab[a][1] .. '.' .. tab[a][2] .. ' = ' .. target_table .. '.' .. tab[a][2] .. ') OR (' .. tab[a][1] .. '.' .. tab[a][2] .. ' IS NULL AND ' .. target_table .. '.' .. tab[a][2] ..' IS NULL))'}
				end

			elseif findRelation(tab[a][1], columns, 1) ~= false then

				if tab[a][5] == false then
					local num = findRelation(tab[a][1], columns, 1)
					columns[num][2] = columns[num][2] .. ', ' .. tab[a][2]
					columns[num][3] = columns[num][3] .. ' AND ' .. '((' .. tab[a][1] .. '.' .. tab[a][2] .. ' = ' .. target_table .. '.' .. tab[a][2] .. ') OR (' .. tab[a][1] .. '.' .. tab[a][2] .. ' IS NULL AND ' .. target_table .. '.' .. tab[a][2] ..' IS NULL))'
				end
			end
		end

		for b = 1, #columns do
			insertStatement[#insertStatement+1] = {'INSERT INTO ' .. columns[b][1] .. '(' .. columns[b][2] .. ') (SELECT DISTINCT ' .. columns[b][2] .. ' FROM ' .. target_schema .. '.' .. target_table .. ' WHERE NOT EXISTS (SELECT 1 FROM ' .. columns[b][1] .. ' WHERE (' .. columns[b][3] .. ')))'}
		end
		
		return insertStatement
	end
----------------------------------------------------------------------------------------------
	/**
	** The insert statement is created in different parts in this function then joined together.
	**
	**/
	function makeFactInsertStatements(tab)
		local insertSegment = ''
		local selectSegment = ''
		local whereClauseKeyComparator = ''
		
		for a = 1, #dimensions do
			if a == 1 then
				insertSegment = dimensions[a][4]
				selectSegment = dimensions[a][3]
				whereClauseKeyComparator = 
				'((' .. 
					dimensions[a][1] .. '.' .. dimensions[a][3] .. ' = ' .. given_fact_table_name .. '.' .. dimensions[a][4] .. ') OR (' ..
					dimensions[a][1] .. '.' .. dimensions[a][3] .. ' IS NULL AND ' .. given_fact_table_name .. '.' .. dimensions[a][4] .. ' IS NULL' .. 
				'))' 
			else
				insertSegment = insertSegment .. ', ' .. dimensions[a][4]
				selectSegment = selectSegment .. ', ' .. dimensions[a][3]
					
				whereClauseKeyComparator = whereClauseKeyComparator .. ' AND ' ..
					'((' .. 
					dimensions[a][1] .. '.' .. dimensions[a][3] .. ' = ' .. given_fact_table_name .. '.' .. dimensions[a][4] .. ') OR (' ..
					dimensions[a][1] .. '.' .. dimensions[a][3] .. ' IS NULL AND ' .. given_fact_table_name .. '.' .. dimensions[a][4] .. ' IS NULL' .. 
					'))' 
			end
		end

		insertStatement = 
			'INSERT INTO ' .. string.upper(given_fact_table_name) .. ' (' .. insertSegment .. ', ' .. factTable[3][1] .. ') (SELECT ' .. selectSegment .. ', ' .. factTable[2][1] ..
			' FROM ' .. target_schema .. '.' .. target_table 
	
		local leftJoins = ''
		
		for b = 1, #columns do
			leftJoins = leftJoins .. ' INNER JOIN ' .. columns[b][1] .. ' ON ' .. columns[b][3]
		end
		
		local whereNotExistsClause = ''
		for c = 1, #factTableIndividualColumns do
			if c ~= 1 then
				whereNotExistsClause = whereNotExistsClause .. ' AND '
			end
			
			whereNotExistsClause = whereNotExistsClause .. '((' .. factTableIndividualColumns[c][2] .. ' = ' .. 
			target_table .. '.' .. factTableIndividualColumns[c][2] .. ') OR (' .. factTableIndividualColumns[c][2] .. ' IS NULL AND ' .. 
			target_table .. '.' .. factTableIndividualColumns[c][2] .. ' IS NULL))'
		end
		
		insertStatement = insertStatement .. leftJoins .. ' WHERE NOT EXISTS (SELECT 1 FROM ' .. given_fact_table_name .. ' WHERE (' .. whereClauseKeyComparator .. ') AND (' .. whereNotExistsClause .. ')))'
		
		return insertStatement
	end
----------------------------------------------------------------------------------------------
	--These are the main functions to run the whole process in the script
----------------------------------------------------------------------------------------------
	/**
	** This is where all the functions are called in a specific order. There are functions which require an input from another function and there are some which use some of the global variables set at the top.
	**
	**/
	local columnsFromUser = separateNumbersFromColumns()
	
	local columnsFromSystem = getColumns(target_schema, raw_target_table)

	local finalTab = checkGivenColNamesWithFlat(columnsFromSystem, columnsFromUser)

	separeteDimensions(finalTab)
	local dimCreateStat = makeDimCreateStatements()
	local factCreateStat = makeFactCreateStatements()
	
	local dimInsertStat = makeDimInsertStatements(finalTab)
	local factInsertStat = makeFactInsertStatements(finalTab)
----------------------------------------------------------------------------------------------
	--If statement to create the dimension and fact tables
----------------------------------------------------------------------------------------------
	if do_create_tables then
		ok, res = pquery('DESCRIBE ' .. given_fact_table_name)
		
		if ok then
			output('Fact table already exists!')
		else
			for a = 1, #dimCreateStat do
				query(dimCreateStat[a][1])
			end
			query(factCreateStat)
		end
	end
----------------------------------------------------------------------------------------------
	--If statement to populate the dimension and fact tables
----------------------------------------------------------------------------------------------
	if transfer_data then
		for a = 1, #dimInsertStat do
			query(dimInsertStat[a][1])
		end
		query(factInsertStat)
	end
----------------------------------------------------------------------------------------------
	--These output statements are only used for debugging
----------------------------------------------------------------------------------------------
	/**
	**
	**
	**/
	local statusOutput = true

	if statusOutput then
		showTable(columnsFromUser, 'separateNumbersFromColumns()')

		showTable(columnsFromSystem, 'getColumns(target_schema, target_table)')

		showTable(finalTab, 'checkGivenColNamesWithFlat(p1, p2)')

		showTable(dimensions, 'separeteDimensions(p1)')

		showTable(dimensions, 'lua table: dimensions')

		showTable(factTable, 'lua table: factTable [global variable]')

		showTable(columns, 'lua table: columns [global variable]')

		showTable(factTableIndividualColumns, 'lua table: factTableIndividualColumns [global variable]')

		showTable(dimCreateStat, 'makeDimCreateStatments(p1)')

		printStr(factCreateStat, 'makeFactCreateStatements(p1)')

		showTable(dimInsertStat, 'makeDimInsertStatements(p1)')

		printStr(factInsertStat, 'makeFactInsertStatements(p1)')
	end
----------------------------------------------------------------------------------------------	
	function merge_tables(t1, t2)
		for i = 1, #t2 do 
			t1[#t1+1] = t2[i]
		end
		return t1
	end
	
	function getOverallSQLStatements()
		local overall_res = {}
		local factCreate = {}
		local factInsert = {}
		
		factCreate[1] = {factCreateStat}
		factInsert[1] = {factInsertStat}
		
		overall_res = merge_tables(overall_res, dimCreateStat)
		overall_res = merge_tables(overall_res, factCreate)
		overall_res = merge_tables(overall_res, dimInsertStat)
		overall_res = merge_tables(overall_res, factInsert)
		
		return overall_res
	end

	exit(getOverallSQLStatements(), "SQL_Statements LONG VARCHAR")
----------------------------------------------------------------------------------------------
/

EXECUTE SCRIPT CONVERT_TO_STAR_SCHEMA('CHALLENGE.FLAT', 'FACT_TABLE', FALSE, FALSE,
	ARRAY(
		'DimProduct',
		'DimSupplier',
		'DimOrder'
	),
	
        ARRAY(
		'1|PROD_NAME',
		'1|PROD_DESC',
		'2|SUPP_NAME',
		'2|SUPP_ADD',
		'3|ORDER_NAME'
	)
) WITH OUTPUT;
