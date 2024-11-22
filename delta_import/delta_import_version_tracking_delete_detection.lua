CREATE LUA SCRIPT "DELTA_IMPORT" (exa_connection,etl_repo_schema,etl_repo_table,etl_logging_table,source_db,source_schema,target_schema) RETURNS TABLE AS
-- Global variables
res = {}
resStruct="SCHEMA_NAME varchar(128), TABLE_NAME varchar(128), ROWS_INSERTED integer, ROWS_UPDATED integer, ROWS_DELETED integer, LAST_VERSION decimal(19)"




/*
  Purpose  : Loggs errors and rolls back the queries executed before the error
  Parameter: error_message - The error message to log
  Return   : If an error occurred, the script will be terminated and prints out an error message (type: varchar).
*/
function exit_error(error_message)
        query("ROLLBACK")
        output("[SQL] ROLLBACK")
        
        suc, updateMessage = pquery([[insert into ::s.::t values (CURRENT_TIMESTAMP(), :ts,:tt,null,null,null,:e)]],{s=etl_repo_schema, t=etl_logging_table, ts = target_schema, tt=targetTable, e=error_message})
        output("[SQL] "..updateMessage.statement_text)
        
        query("COMMIT")
        output("[SQL] COMMIT")
        output("[ERROR] "..error_message) 
        
        exit({{error_message}}, "ERROR varchar(1000)")
end

/*
  Purpose  : Evaluates the return state of a pquery function call.
  Parameter: suc - The success indicator. This can be either 'true' or false'.
             err - The error message. If no error occurred, the value of that variable is empty.
  Return   : If an error occurred, the script will be terminated and prints out an error message (type: varchar).
*/
function EvalQuery(suc, err)
    output("[DEBUG] => EvalQuery("..tostring(suc)..")")
    if not suc then      
        exit_error(err)
    end 
    output("[DEBUG] <= EvalQuery()")
end

/*
  Purpose  : Returns the last version number from a MSSQL Server database.
  Parameter: n/a
  Return   : The last version number (type: BIGINT).
*/
function GetLastVersion()
    output("[DEBUG] => GetLastVersion()")
    suc, queryVersion=pquery([[select VERSION from (import from JDBC at ]]..exa_connection..[[ statement 'select CHANGE_TRACKING_CURRENT_VERSION() as VERSION')]])
    if queryVersion == null then
        exit_error("Change tracking not enabled on SQL-Server")
    end
    newVersion=queryVersion[1][1]
    output("[SQL] "..queryVersion.statement_text)
    EvalQuery(suc, queryVersion.error_message)
    output("[DEBUG] <= GetLastVersion("..newVersion..")")

    return newVersion
end

/*
  Purpose  : Returns the minimum version on the client that is valid for use in obtaining change tracking information from the specified MSSQL Server table.
  Parameter: srcTab - The name of the source table to be checked 
  Return   : The minimum version number of the specified table (type: BIGINT).
*/
function GetMinVersion(srcTab)
    output("[DEBUG] => GetMinVersion()")
    suc, queryMinVersion=pquery([[select MIN_VERSION from (import from JDBC at ]]..exa_connection..[[ statement 'select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('']]..source_schema..'.'..srcTab..[[''))as MIN_VERSION')]])
    if queryMinVersion == null then
        exit_error("Change tracking not enabled on SQLServer for table " ..source_schema..'.'..srcTab )
    end
    
    minVersion=queryMinVersion[1][1]
    if minVersion == null then
        exit_error("[ERROR] Change tracking not enabled on SQLServer for table " ..source_schema..'.'..srcTab ) 
    end
    output("[SQL] "..queryMinVersion.statement_text)
    EvalQuery(suc, queryMinVersion.error_message)
    output("[DEBUG] <= GetMinVersion("..minVersion..")")

    return minVersion
end

/*
  Purpose  : Updates the last version number in the ETL control table.
  Parameter: newLastVersion - The latest version number to be stored in the ETL control table
             sourceTable    - The name of the table in the MSSQL Server database.
             targetTable    - The name of the table in Exasol.
  Return   : n/a
*/
function UpdateLastVersion(newLastVersion, sourceTable, targetTable)
    output("[DEBUG] => UpdateLastVersion("..newLastVersion..", "..sourceTable..", "..targetTable..")")
    if lastVersion ~= newLastVersion then
        suc, updateVersion=pquery([[update ::es.::et set LAST_VERSION=:v where SOURCE_DB=:d and SOURCE_SCHEMA=:s1 and SOURCE_TABLE=:t1 and TARGET_SCHEMA=:s2 and TARGET_TABLE=:t2]], {es=quote(etl_repo_schema),et=quote(etl_repo_table), v=newLastVersion, d=source_db, s1=source_schema, s2=target_schema, t1=sourceTable, t2=targetTable})
        output("[SQL] "..updateVersion.statement_text)
        EvalQuery(suc, updateVersion.error_message)
    end
    output("[DEBUG] <= UpdateLastVersion()")
end

/*
  Purpose  : Get the column names from SQL-Server with the conversion that needs to be applied when loading into Exasol
  Return   : String containing the converted column names, with T. in front 
             (Type:VARCHAR)
             Example for return value: 'CONVERT(uniqueidentifier, [T.col1] ),[T.col2]'
*/
function GetColumnTypes(exa_connection,source_db,source_schema,sourceTable)
        output("[DEBUG] => GetColumnTypes()")
        suc, colTypes = pquery([[
        with sqlserv_base as(
                select * from(
                        import from JDBC at ]]..exa_connection..[[ statement 'select    '']]..source_db..[[''  as DB_NAME,
                 s.name  as SCHEMA_NAME,
                 t.name  as TABLE_NAME,
                c.column_id as COLUMN_ID,c.name  as COLUMN_NAME,
                c.max_length as COL_MAX_LENGTH,
                c.precision as PRECISION,
                c.scale as SCALE,
                c.is_nullable as IS_NULLABLE,
                c.is_identity as IS_IDENTITY,
                c.system_type_id as SYSTEM_TYPE_ID,
                c.user_type_id as USER_TYPE_ID,
                ty.name as TYPE_NAME
                        from ]]..source_db..[[.sys.schemas s
          join ]]..source_db..[[.sys.tables t on s.schema_id=t.schema_id
          join ]]..source_db..[[.sys.columns c on c.object_id=t.object_id
          join sys.types ty on c.user_type_id = ty.user_type_id
                                                where s.name = ('']]..source_schema..[['') and t.name = ('']]..sourceTable..[['') '
                                        )
                                )
          select group_concat(
          case USER_TYPE_ID -- SQLSERVER datatype system type codes are in system table SYS.TYPES,
                    when 41 then 'cast(T.[' || column_name || '] as DateTime) AS ' || '[' || column_name || ']'  --time
                    when 43 then 'CONVERT(datetime2, T.[' || column_name || '], 1) AS ' || '[' || column_name || ']' --datetimeoffset
                    when 128 then 'T.[' || column_name || '].ToString() AS ' || '[' || column_name || ']'
                    when 129 then 'T.[' || column_name || '].ToString() AS ' || '[' || column_name || ']'
                    when 130 then 'T.[' || column_name || '].ToString() AS ' || '[' || column_name || ']'
                    when 189 then 'CAST(T.[' || column_name || '] AS DATETIME) AS ' || '[' || column_name || ']'
                    when 173 then 'CONVERT(uniqueidentifier, T.[' || column_name || '] ) AS ' || '[' || column_name || ']' -- binary to guid
                    when 165 then 'CONVERT(VARCHAR(2000), T.[' || column_name || '] ) AS ' || '[' || column_name || ']'  -- VARBINARY to VARCHAR
                    when 34 then '''''X'''' AS ' || '[' || column_name || ']' -- image to varchar - will not be imported
                    when 98 then '''''X'''' AS ' || '[' || column_name || ']' -- sql_variant to varchar - will not be imported
                    else 'T.[' || column_name || ']' -- all non-special datatypes '
                end order by COLUMN_ID SEPARATOR ',' ) as CONV_COLS_SQLSERVER,
                group_concat(
          case USER_TYPE_ID -- SQLSERVER datatype system type codes are in system table SYS.TYPES,
                    when 41 then 'CAST("' || column_name || '" AS ' ||' ' ||'TIMESTAMP) AS "' || column_name || '"'       --time
                    when 43 then 'CAST("' || column_name || '" AS ' ||' ' ||'TIMESTAMP) AS "' || column_name || '"' --datetimeoffset
                    when 128 then 'CAST("' || column_name || '" AS ' ||' ' ||'VARCHAR(2000000)) AS "' || column_name || '"' --hierarchyid
                    when 129 then 'CAST("' || column_name || '" AS ' ||' ' ||'GEOMETRY) AS "' || column_name || '"' --geometry
                    when 130 then 'CAST("' || column_name || '" AS ' ||' ' ||'GEOMETRY) AS "' || column_name || '"' --geography
                    when 189 then 'CAST("' || column_name || '" AS ' ||' ' ||'TIMESTAMP) AS "' || column_name || '"'  -- timestamp
                    when 165 then 'CAST("' || column_name || '" AS ' ||' ' ||'VARCHAR(2000000)) AS "' || column_name || '"' -- varbinary to varchar
                    when 173 then 'CAST("' || column_name || '" AS ' ||' ' ||'HASHTYPE('||case when COL_MAX_LENGTH <= 1024 then CEILING(COL_MAX_LENGTH/8)*8 else NULL end ||' BYTE)) AS "' || column_name || '"'        -- BINARY 16 aka GUID
                    when 34 then 'CAST("' || column_name || '" AS ' ||' ' ||'VARCHAR(2000000)) AS "' || column_name || '"' -- image to varchar - placeholder value, data will not be imported
                    when 98 then 'CAST("' || column_name || '" AS ' ||' ' ||'VARCHAR(2000000)) AS "' || column_name || '"' -- sql_variant to varchar - placeholder value, will not be imported
                    else '"' || column_name || '"' -- all non-special datatypes '
                end order by COLUMN_ID SEPARATOR ',' ) as CONV_COLS_EXASOL
          from sqlserv_base
        
        ]])
        output("[SQL] "..colTypes.statement_text)
        EvalQuery(suc, colTypes.error_message)
        colTypesSQLServer=colTypes[1][1]
        colTypesExasol=colTypes[1][2]
        if colTypes == null then
                exit_error("[ERROR] Could not get column types for " ..source_schema..'.'..sourceTable ) 
        end
        output("[DEBUG] <= GetColumnTypes()")
        return colTypesSQLServer, colTypesExasol

end


-- Function to get the current timestamp in 'YYYYMMDDHHMMSSSSS' format
function getCurrentTimestampFormatted()
    local dateTable = os.date("*t")  -- Get current date and time
    -- Format the date and time into 'YYYYMMDDHHMMSS' and append '000' for milliseconds
    local formattedTimestamp = string.format(
        "%04d%02d%02d%02d%02d%02d000", 
        dateTable.year, 
        dateTable.month, 
        dateTable.day, 
        dateTable.hour, 
        dateTable.min, 
        dateTable.sec
    )
    return formattedTimestamp
end
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

targetTable = 'global' -- predefine value for error messages

-- Query ETL control table to determine source and target table and the change version number
suc, queryRepo=pquery([[select SOURCE_TABLE, TARGET_TABLE, LAST_VERSION from ::es.::et where SOURCE_DB=:d and SOURCE_SCHEMA=:s and TARGET_SCHEMA=:t]], {es=quote(etl_repo_schema), et=quote(etl_repo_table), d=source_db, s=source_schema, t=target_schema})
output("[SQL] "..queryRepo.statement_text)
EvalQuery(suc, queryRepo.error_message)

--
-- Table load
--
for i=1, #queryRepo do

    local insertedRows=0
    local updatedRows=0
    local deletedRows=0
    sourceTable=queryRepo[i][1]
    targetTable=queryRepo[i][2]
    lastVersion=queryRepo[i][3]
    if lastVersion == null then
        lastVersion=0
    end
    output(" ++ Source table "..sourceTable)
    output(" ++ Target table: "..targetTable)
    output(" ++ Last version: "..lastVersion)
    
    
     ---------- Autofill -------------------------------------------------
        -- are the columns that should be filled automatically present in the table?
        -- count must be 5 for logic to kick in
        suc, queryAutofill = pquery([[select count(column_name) as cnt from exa_all_columns where column_schema=']]..target_schema..[[' and column_table=']]..targetTable..[[' and column_name in ('SYS_DWH_CHANGE_TIMESTAMP','SYS_DWH_LOAD_TIMESTAMP','SYS_DWH_LOAD_SOURCE','SYS_DWH_LOAD_DIRECTION', 'SYS_DWH_CHANGE_TYPE')]])
        output("[SQL] "..queryAutofill.statement_text)
        EvalQuery(suc, queryAutofill.error_message)
        
        if queryAutofill[1][1] == 5 then
                output(" ++ Autofill enabled")
                autofill = true
                -- this statement is used to filter out the columns that should be filled via auto-fill
                autoFillColsStmt = [[ and column_name not in ('SYS_DWH_CHANGE_TIMESTAMP','SYS_DWH_LOAD_TIMESTAMP','SYS_DWH_LOAD_SOURCE','SYS_DWH_LOAD_DIRECTION', 'SYS_DWH_CHANGE_TYPE')]]
                autoFillVals = [[,SYS_DWH_CHANGE_TIMESTAMP,SYS_DWH_LOAD_TIMESTAMP,SYS_DWH_LOAD_SOURCE,SYS_DWH_LOAD_DIRECTION, SYS_DWH_CHANGE_TYPE]]
                autoFillMerge = [[/*autofill*/ ,SYS_DWH_CHANGE_TIMESTAMP = current_timestamp(), SYS_DWH_LOAD_TIMESTAMP = current_timestamp(), SYS_DWH_LOAD_SOURCE = ']]..exa_connection..[[', SYS_DWH_LOAD_DIRECTION = 'I', SYS_DWH_CHANGE_TYPE = 'U']]
                autoFillMergeInsert = [[/*autofill*/, current_timestamp(), current_timestamp(), ']]..exa_connection..[[', 'I', 'I']]
                -- NEW: For deletions
                autoFillDelete = [[SYS_DWH_CHANGE_TIMESTAMP = current_timestamp(), SYS_DWH_LOAD_TIMESTAMP = current_timestamp(), SYS_DWH_LOAD_SOURCE = ']]..exa_connection..[[', SYS_DWH_LOAD_DIRECTION = 'I', SYS_DWH_CHANGE_TYPE = 'D']]
                
        elseif queryAutofill[1][1] == 0 then
                output(" ++ Only "..queryAutofill[1][1].." columns, no autofill")
                autofill = false
                autoFillColsStmt = [[]] -- nothing additional needed here
                autoFillVals = [[]]
                autoFillMerge = [[]]
                autoFillMergeInsert = [[]]
                autoFillDelete = [[]]
        else -- some autofill columns but not all --> something is wrong
                exit_error("Table contains some autofill columns but not all. Please check DDL of Exasol-table " ..target_schema..'.'..targetTable )
        end
        
        -- get all columns without the ones for autofill
        suc, colsWOAutofill = pquery([[select group_concat('"' || column_name || '"' order by column_ordinal_position) from exa_all_columns where column_schema=']]..target_schema..[[' and column_table=']]..targetTable..[[' ]]..autoFillColsStmt)
        output("[SQL] "..colsWOAutofill.statement_text)
        EvalQuery(suc, colsWOAutofill.error_message)
        exaColsWOAutofill = colsWOAutofill[1][1]
        if exaColsWOAutofill == null then
                exit_error("No columns for table "..target_schema..'.'..targetTable )
        end
        allExaCols =  exaColsWOAutofill.. autoFillVals
        
        ---------- Autofill end -------------------------------------------------
        
        
        
    convertedColTypesSQLServer, convertedColTypesExasol = GetColumnTypes(exa_connection,source_db,source_schema,sourceTable)

    if lastVersion == 0 then
        -- Step 1: Import data without affecting timestamps
        suc, importData = pquery([[import into ]] .. quote(target_schema) .. '.' .. quote(targetTable) ..
                                 [[ (]] .. exaColsWOAutofill .. [[) 
                                 from JDBC at ]] .. exa_connection .. 
                                 [[ statement 'select ]] .. convertedColTypesSQLServer .. 
                                 [[ from ]] .. source_schema .. '.' .. sourceTable .. [[ T']])
        output("[SQL] " .. importData.statement_text)
        EvalQuery(suc, importData.error_message)
        insertedRows = importData.rows_inserted
    
        -- Step 2: Update the timestamps for the newly imported records where timestamps are NULL
        suc, timestampUpdate = pquery([[UPDATE ]] .. quote(target_schema) .. '.' .. quote(targetTable) .. 
                                      [[ SET SYS_DWH_LOAD_TIMESTAMP = current_timestamp(), 
                                      SYS_DWH_CHANGE_TIMESTAMP = current_timestamp(), 
                                      SYS_DWH_LOAD_SOURCE = ']] .. exa_connection .. 
                                      [[', SYS_DWH_LOAD_DIRECTION = 'I', 
                                      SYS_DWH_CHANGE_TYPE = 'I' 
                                      WHERE SYS_DWH_LOAD_TIMESTAMP IS NULL]])
        output("[SQL] " .. timestampUpdate.statement_text)
        EvalQuery(suc, timestampUpdate.error_message)
        newLastVersion=math.maxinteger

    elseif lastVersion < GetMinVersion(sourceTable) then
        output('Full load of table '.. sourceTable)
        
        -- Truncate target table
        suc, truncateTable=pquery([[truncate table ]]..quote(target_schema)..'.'..quote(targetTable)..[[]])
        output("[SQL] "..truncateTable.statement_text)
        EvalQuery(suc, truncateTable.error_message)
        
        -- Import source table records into target table
        suc, importData=pquery([[import into ]]..quote(target_schema)..'.'..quote(targetTable)..[[(]]..exaColsWOAutofill..[[) from JDBC at ]]..exa_connection..[[ statement 'select ]]..convertedColTypesSQLServer..[[ from ]]..source_schema..'.'..sourceTable..[[ T']])
        output("[SQL] "..importData.statement_text)
        EvalQuery(suc, importData.error_message)
        insertedRows=importData.rows_inserted
        
        -- Update the autofill cols if they are present
        if autofill then
                suc, autof = pquery([[UPDATE ]]..quote(target_schema)..'.'..quote(targetTable)..[[ SET SYS_DWH_CHANGE_TIMESTAMP = current_timestamp(), SYS_DWH_LOAD_TIMESTAMP = current_timestamp(), SYS_DWH_LOAD_SOURCE = ']]..exa_connection..[[', SYS_DWH_LOAD_DIRECTION = 'I', SYS_DWH_CHANGE_TYPE = 'I']])
                output("[SQL] "..autof.statement_text)
                EvalQuery(suc, autof.error_message)
        end
        
        -- Determine the new last version number
        newLastVersion=GetLastVersion()
    else
        output('Delta load of table '..sourceTable)

        -- Query source table PK columns name
        suc, queryPK=pquery([[select group_concat(PK) as PKs,
                                group_concat('T.['|| PK ||'] = CT.['|| PK ||']' SEPARATOR ' AND ') AS PK_SQLSERVER,
                                group_concat('src."'|| PK ||'" = tgt."'|| PK ||'"' SEPARATOR ' AND ') AS PK_EXASOL
                        from (import from JDBC at ]]..exa_connection..[[ statement 'select K.COLUMN_NAME as PK from INFORMATION_SCHEMA.TABLE_CONSTRAINTS T join INFORMATION_SCHEMA.KEY_COLUMN_USAGE K on K.CONSTRAINT_NAME=T.CONSTRAINT_NAME where K.TABLE_NAME='']]..sourceTable..[['' and K.TABLE_SCHEMA='']]..source_schema..[['' and T.CONSTRAINT_TYPE=''PRIMARY KEY''')]])
        output("[SQL] "..queryPK.statement_text)
        EvalQuery(suc, queryPK.error_message)
        output(" ++ PK columns: "..queryPK[1][1])



        -- Query non PK source columns and build filter condition accordingly        
        pk_query_text = [[select PK from (import from JDBC at ]]..exa_connection..[[ statement 'select K.COLUMN_NAME as PK from INFORMATION_SCHEMA.TABLE_CONSTRAINTS T join INFORMATION_SCHEMA.KEY_COLUMN_USAGE K on K.CONSTRAINT_NAME=T.CONSTRAINT_NAME where K.TABLE_NAME='']]..sourceTable..[['' and K.TABLE_SCHEMA='']]..source_schema..[['' and T.CONSTRAINT_TYPE=''PRIMARY KEY''')]]
        suc, queryNonPkColumns=pquery([[with tmp_1 as (select column_name from exa_all_columns where column_schema=']]..target_schema..[[' and column_table=']]..targetTable..[[' ]].. autoFillColsStmt ..[[ minus (]]..pk_query_text..[[) ) select group_concat('tgt."' || column_name || '" = src."' || column_name || '"' SEPARATOR ', ') COLLIST from tmp_1]])
        output("[SQL] "..queryNonPkColumns.statement_text)
        EvalQuery(suc, queryNonPkColumns.error_message)
        nonPkColumns=queryNonPkColumns[1][1]
        
        -- Query source columns and build list of columns to be inserted
        suc, queryColumns=pquery([[select group_concat('src."'||column_name||'"' ORDER BY COLUMN_ORDINAL_POSITION) COLS from exa_all_columns where column_schema=']]..target_schema..[[' and column_table=']]..targetTable..[[' ]].. autoFillColsStmt)
        output("[SQL] "..queryColumns.statement_text)
        EvalQuery(suc, queryColumns.error_message)
        columns=queryColumns[1][1]
        
        suc, mergeData=pquery([[merge into ]]..quote(target_schema)..'.'..quote(targetTable)..[[ tgt using (select ]]..convertedColTypesExasol..
                                [[ from (import from JDBC at ]]..exa_connection..[[ statement 'select ]]..convertedColTypesSQLServer..[[ from ]]..source_schema..'.'..sourceTable..[[ as T join (SELECT * FROM CHANGETABLE(CHANGES ]]..source_schema..'.'..sourceTable..[[, ]]..lastVersion..[[) CTL WHERE CTL.SYS_CHANGE_OPERATION IN (''I'', ''U'')) AS CT ON ]]..queryPK[1][2]..[[' )) src on ]]..queryPK[1][3]..
                                [[ when matched then update set ]]..nonPkColumns..autoFillMerge..[[ where ]]..queryPK[1][3]..[[ when not matched then insert (]]..allExaCols..[[) values (]]..columns..autoFillMergeInsert..[[)]])
        
        -- Output for debugging
        output("[SQL] " .. mergeData.statement_text)
        EvalQuery(suc, mergeData.error_message)
        insertedRows=mergeData.rows_inserted
        updatedRows=mergeData.rows_updated
            
        -- NEW: Deleted rows
        -- detect primary keys
        -- Split primary keys into a table
        -- Assuming queryPK[1][1] contains the primary key columns like 'GUID, ORGANIZATIONALUNIT'
        
        
        -- Split primary keys into a table
        local primaryKeys = {}
        for pk in string.gmatch(queryPK[1][1], "[^,]+") do
            primaryKeys[#primaryKeys + 1] = pk:match("^%s*(.-)%s*$")  -- Trim whitespace
        end
        local primaryKeysList = table.concat(primaryKeys, ", ")
        
        
        -- Create a table to store added columns to avoid duplicates
        local addedColumns = {}
        
        -- Filter SQL Server Columns: Handle both CONVERT and plain column references
        local convertedPKsSQL = {}
        for _, pk in ipairs(primaryKeys) do
            -- Handle CONVERT columns
            
            for col in string.gmatch(convertedColTypesSQLServer, 'CONVERT%([^,]+, T%.[^,]+%) AS %[' .. pk .. '%]') do
                if not addedColumns[pk] then
                    convertedPKsSQL[#convertedPKsSQL + 1] = col  -- Add the full CONVERT expression for the primary key
                    addedColumns[pk] = true  -- Mark this column as added
                end
            end
            -- Handle plain column references (no CONVERT)
            for col in string.gmatch(convertedColTypesSQLServer, 'T%.%[' .. pk .. '%]') do
                if not addedColumns[pk] then
                    convertedPKsSQL[#convertedPKsSQL + 1] = col  -- Add the plain column reference
                    addedColumns[pk] = true  -- Mark this column as added
                end
            end
        end
        
        -- Concatenate SQL Server columns
        local convertedPKSQLServer = table.concat(convertedPKsSQL, ", ")
        
        -- Reset the addedColumns table for Exasol handling
        addedColumns = {}
        
        -- Filter Exasol Columns: Handle both CAST and plain column references
        local convertedPKsExasol = {}
        for _, pk in ipairs(primaryKeys) do
            -- Handle CAST columns
            
            for col in string.gmatch(convertedColTypesExasol, 'CAST%([^,]+ AS [^,]+%) AS "' .. pk .. '"') do
                if not addedColumns[pk] then
                    convertedPKsExasol[#convertedPKsExasol + 1] = col  -- Add the full CAST expression for the primary key
                    addedColumns[pk] = true  -- Mark this column as added
                end
            end
            -- Handle plain column references (no CAST)
            for col in string.gmatch(convertedColTypesExasol, '"' .. pk .. '"') do
                if not addedColumns[pk] then
                    convertedPKsExasol[#convertedPKsExasol + 1] = col  -- Add the plain column reference
                    addedColumns[pk] = true  -- Mark this column as added
                end
            end
        end
        
        -- Concatenate Exasol columns
        local convertedPKExasol = table.concat(convertedPKsExasol, ", ")
        
        -- Debug: Output the primary key columns for SQL Server and Exasol
        output("[Primary Key Columns in SQL Server] " .. convertedPKSQLServer)
        output("[Primary Key Columns in Exasol] " .. convertedPKExasol)

        
        local mergeData_delete_query = [[merge into ]]..quote(target_schema)..'.'..quote(targetTable)..[[ tgt using (select ]]..convertedPKExasol..
                                            [[ , SYS_CHANGE_OPERATION from (import from JDBC at ]]..exa_connection..[[ statement 'select ]]..convertedPKSQLServer..[[, T.SYS_CHANGE_OPERATION from CHANGETABLE(CHANGES ]]..source_schema..'.'..sourceTable..[[, ]]..lastVersion..[[) AS T WHERE T.SYS_CHANGE_OPERATION = ''D'' ')) src ON (]]..queryPK[1][3]..[[ AND tgt.SYS_DWH_CHANGE_TYPE != 'D') -- Check if record is already marked as deleted
                                                 when matched then update set ]]..autoFillDelete..[[ where ]]..queryPK[1][3]
        
        suc_delete, mergeData_delete=pquery(mergeData_delete_query)
        
        -- Output for debugging
        output("[SQL] " .. mergeData_delete.statement_text)
        EvalQuery(suc_delete, mergeData_delete.error_message)
        -- Capture the number of affected rows (i.e., deleted rows)
        deletedRows = mergeData_delete.rows_updated -- This should capture the number of rows marked as deleted
    
        -- Determine the new last version number
        if insertedRows == 0 and updatedRows == 0 and deletedRows == 0 then
            -- Nothing has changed
            newLastVersion=lastVersion
        else
          newLastVersion=GetLastVersion()
        end
    end
    
    -- Update change version number in ETL control table
    UpdateLastVersion(newLastVersion, sourceTable, targetTable)
    
    suc, updateMessage = pquery([[insert into ::s.::t values (CURRENT_TIMESTAMP(), :ts,:tt,:i,:u,:d,:n,'')]],{s=etl_repo_schema, t=etl_logging_table, ts = target_schema, tt=targetTable, i=insertedRows,u=updatedRows,d=deletedRows,n=newLastVersion})
    EvalQuery(suc, updateMessage.error_message)
    -- Build result table
    res[i]={target_schema, targetTable, insertedRows, updatedRows, deletedRows, newLastVersion}
end

return res, resStruct
