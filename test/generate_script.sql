create schema if not exists database_migration;

--/
CREATE OR REPLACE LUA SCRIPT DATABASE_MIGRATION.GENERATE_SCRIPT(content) RETURNS TABLE AS



-- checks whether the content of tokenlist starting at strt is a script
-- returns: boolean - isScript, String - scriptType, String -scriptName,  String -scriptComment
function isScript(tokenlist, strt)
	
	scriptType = 'LUA' -- LUA is the default, other possible values are: JAVA, PYTHON, R
	scriptComment = '' -- Collect comments before 'CREATE'-keyword in this String

	-- search for first token that is no comment or whitespace
	while( sqlparsing.iswhitespaceorcomment(tokenlist[strt]) and strt < #tokenlist) do
		scriptComment = scriptComment .. tokenlist[strt]
		strt = strt + 1
	end

	if(sqlparsing.normalize(tokenlist[strt]) ~= "CREATE") then
		return false, 'NONE' , '', ''
	end

	-- after a create, the next 7 tokens must contain 'script' in order to be a proper script
	-- Maximum number of tokens between CREATE and SCRIPT: 7
	-- CREATE OR REPLACE SCALAR LUA SCRIPT
	nr_search_to = math.min(#tokenlist, strt + 7)
	output('search from '..strt..' until token '..nr_search_to)

	-- use while-loop instead of for-loop here because nr_search_to gets changed in loop
	i = strt + 1;
	while( i <= nr_search_to) do

		-- determine script type. must be explicitly  given if other than lua
		if (tokenlist[i] == 'PYTHON') or (tokenlist[i] == 'JAVA') or (tokenlist[i] == 'R') then
			scriptType = tokenlist[i]
		end
		
		-- if there's a comment token, enlarge the search space by one
		if(sqlparsing.iswhitespaceorcomment(tokenlist[i]) ) then
			
			if (nr_search_to < #tokenlist) then
				output('Whitespace or comment in script search space --> skip token and enlarge search space by one')
				nr_search_to = nr_search_to +1
			end
		else
			if(string.find(tokenlist[i], ';')) then
				output('found semicolon before "SCRIPT"-keyword --> no script')
				return false, 'NONE', '', ''
			end
			output('search for "SCRIPT" in '.. sqlparsing.normalize(tokenlist[i]))
			if (sqlparsing.normalize(tokenlist[i]) == "SCRIPT") then
				-- find token containing script name
				name_search_pos = i + 1
				while( (not sqlparsing.isidentifier(tokenlist[name_search_pos])) and name_search_pos < #tokenlist) do
					name_search_pos = name_search_pos + 1
				end
				scriptName = tokenlist[name_search_pos]
				return true, scriptType, scriptName, scriptComment
			end
		end
	i = i +1
	end -- end while-loop
	return false, 'NONE', '', ''

end

---------------------------------------------------------------------------------

-- returns token number of '/' if tokenstring contains newline followed by '/'
function findScriptEnd(tokenlist, startToken)
	for i = startToken + 1, #tokenlist do
		if (string.find(tokenlist[i-1], '\n') ~= nil and tokenlist[i] == '/') or tokenlist[i] == '\n/\n' then
			return i
		end
	end
	return nil
end

---------------------------------------------------------------------------------

-- split by delimiter into array and include delimiter also in the array
function split(str, delim)
   -- Eliminate bad cases...
   if string.find(str, delim) == nil then
      return { str }
   end

   local result = {}
   local pat = "(.-)" .. delim .. "()"
   local nb = 0
   local lastPos
   for part, pos in string.gfind(str, pat) do
      nb = nb + 1
      result[nb] = part
	  nb = nb + 1
	  result[nb] = delim
      lastPos = pos

   end
   -- Handle the last field
	rest = string.sub(str, lastPos)
	if( rest ~= nil and #rest ~= 0) then
		result[nb + 1] = rest
	end

   return result
end

---------------------------------------------------------------------------------
-- create a new tokenlist that is also splitted by CRLF / CRLF
function splitBySlash(tokenlist)
-- create a string that contains a CRLF
scriptEndToken = '\n/\n'

	new_tokenlist = {}
	for i = 1, #tokenlist do
		if string.find(tokenlist[i],scriptEndToken) then
			splitted = split(tokenlist[i], scriptEndToken)
			for j = 1, #splitted do
				table.insert(new_tokenlist, splitted[j])
			end
		else
			table.insert(new_tokenlist, tokenlist[i])
		end
	end
return new_tokenlist
end

---------------------------------------------------------------------------------
function getStatements(script_file)
	statements = {}

	tokenlist = sqlparsing.tokenize(script_file)
	tokenlist = splitBySlash(tokenlist)
	
	startTokenNr = 1
	searchForward = true
	searchSameLevel = false
	ignoreFunction = sqlparsing.iswhitespaceorcomment
	stmtEnd = ';'
	skriptStart= 'CREATE'

-- Debugging --------
-- output('---- TOKENLIST START ----')
-- 	for i = 1, #tokenlist do
-- 		output(i..'	'..tokenlist[i])
-- 	end
-- output('---- TOKENLIST END ----')
-- Debugging --------
	
	
	while startTokenNr <= #tokenlist do
		
		-- check if the next statement is a script
		stmtIsScript, scriptType, scriptName, scriptComment = isScript(tokenlist, startTokenNr)
		if (stmtIsScript) then
			output('---> is script. Search for / starting at '.. startTokenNr)
			-- check whether token before / is a newline, if not, it's no proper script end
			endTokenNr = findScriptEnd(tokenlist, startTokenNr)
			if endTokenNr ~= nil then
				output('End token nr is '..endTokenNr..' text: '..tokenlist[endTokenNr])
			end

		else
			output('---> is NO script. Search for '..stmtEnd..' starting at '.. startTokenNr)
			endTokenNr = sqlparsing.find(tokenlist, startTokenNr, searchForward, searchSameLevel, ignoreFunction, stmtEnd)
			if endTokenNr ~= nil then
				endTokenNr = endTokenNr[1]
				output('End token nr is '..endTokenNr..' text: '..tokenlist[endTokenNr])
			end
		end
		
		if endTokenNr == nil then
			output('No endtoken found, setting to #tokenlist: '.. #tokenlist)
			endTokenNr = #tokenlist
		end

		stmt = {unpack(tokenlist, startTokenNr, endTokenNr)}
		stmt = table.concat(stmt, "")
		table.insert(statements, {stmt, stmtIsScript, scriptName, scriptComment})
		startTokenNr = endTokenNr  + 1

	end
	return statements
end



-- extracted from actual script into function to be able to switch quickly between behaviours
function execute_scripts_only(script_statements)
	info = {}
	for j = 1, #script_statements do
		stmt = script_statements[j][1]
		stmt_isScript = script_statements[j][2]
		executed_this_stmt = ''
	
		if ( stmt_isScript) then
			stmt_suc, stmt_res = pquery(stmt)
			if stmt_suc then 
				executed_this_stmt = 'YES'
			else
				executed_this_stmt = 'FAILED: '.. stmt_res.error_message
			end
		end
	
		table.insert(info,{stmt , stmt_isScript, executed_this_stmt})
	end -- end for-loop
	return info
end


function string.startswith(String,Start)
   return string.sub(String,1,string.len(Start))==Start
end

-- append table t2 to t1
function extendTable(t1,t2)
    for i=1,#t2 do
        t1[#t1+1] = t2[i]
    end
    return t1
end

---------------------------------------------------------------------------------
------------------------- actual script -----------------------------------------


script_statements = getStatements(content)
new_info = execute_scripts_only(script_statements)

exit(new_info, "stmt varchar(2000000), stmt_is_script varchar(2000),executed varchar(20000)")

/
/*
DROP SCRIPT database_migration.test_script;
--repository, file_filter, search_recursive
EXECUTE SCRIPT DATABASE_MIGRATION.GENERATE_SCRIPT(
'create or replace table "DATABASE_MIGRATION"."TEST" (sql_text varchar(200000));

INSERT INTO "DATABASE_MIGRATION"."TEST" VALUES (''test''),(''test2'');

create or replace script database_migration.test_script(
) RETURNS TABLE
AS 
suc, res = pquery([[ SELECT * FROM "DATABASE_MIGRATION"."TEST"]])
return(res)
/
execute script database_migration.test_script();'
)
--with output
;

-- drop SCHEMA DATABASE_MIGRATION cascade;
*/
