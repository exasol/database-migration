/*
    The delta-import script syncs tables from a source system to an Exasol database. 
    It's made for use on a regular basis as only new or updated rows will be loaded from source to target.
    Find further explainations in the README file on Github
*/
--/
create or replace script database_migration.delta_import( conn_type, conn_db, conn_name, src_schema, src_table,
                                    tgt_schema, tgt_table, delta_detection_column, 
                                    staging_schema, execute_statements
) returns table
as


-----------------------------------------------------------------------------------------
-- use a function here so that the query_wrapper does proper error logging and the user also gets feedback if he calls the script ineractively
function finish_with_error(error_text)
        wrapper:log('ERROR', error_text, 1)
        -- uncomment this in case you want error messages instead of the log output as result
        -- wrapper:finish()
        -- error(error_text)
        return wrapper:finish()
end
-----------------------------------------------------------------------------------------
-- Helper function to get maximal string length for a column
function getMaxLengthForColumn(input_table, column_number)
    length = 1
    for i=1, #input_table do
        curr_length = #input_table[i][column_number]
        if(curr_length ~= null and curr_length > length) then
            length = curr_length
        end
    end
    return length
end

-----------------------------------------------------------------------------------------
-- create log tables if they do not exist yet
function create_log_tables(log_schema, job_log_table, job_details_table)

query([[create schema if not exists ::s ]], {s= log_schema})
query([[
create table if not exists ::s.::jl(
            run_id int identity,
            script_name varchar(100),
            status varchar(100),
            start_time timestamp default systimestamp,
            end_time timestamp
        )
]], {s= staging_schema, jl=job_log_table})
query[[commit]]

query([[
        create table if not exists ::s.::jd (
            detail_id int identity,
            run_id int,
            log_time timestamp,
            log_level varchar(10),
            log_message varchar(2000000),
            rowcount int
        )
]], {s= log_schema, jd=job_details_table})
query[[commit]]
end

-----------------------------------------------------------------------------------------
function quoteForDb(db_name, string_to_quote)

-- remove quotes if already set
string_to_quote = string.gsub(string_to_quote, '"', '')
escape_char = '"'
    if db_name == 'MYSQL' then
        escape_char = '`'
    end
    return escape_char .. string_to_quote .. escape_char
end

-----------------------------------------------------------------------------------------
-- used to change quotes in a string to match other db quoting style
-- e.g. change 'select "abc" from "def"' to 
-- 'select `abc` from `def`' for MYSQL
function changeQuotesForDb(db_name, string_to_change)

escape_char = '"'
    if db_name == 'MYSQL' then
        escape_char = '`'
        return string.gsub(string_to_change, '"', escape_char)
    else
        return string_to_change
    end
end

-----------------------------------------------------------------------------------------
-- this function returns the statement part that can be added to a query in order 
-- to only get the columns that are present both in the source and target system's tables
function get_src_cols_stmt (conn_type, conn_db, conn_name, src_schema, src_table)

    src_system_stmt = [[]]
    -- get table and column name from src_system
    if (conn_db == 'SQLSERVER') then
        src_system_stmt = [['
        SELECT table_name, column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA like '']]..src_schema..[[''
        AND TABLE_NAME like '']]..src_table..[[''
        ']]

    elseif (conn_db == 'ORACLE') then
        src_system_stmt = [['
        SELECT    table_name, column_name
        FROM    all_tab_columns
        WHERE    owner LIKE '']]..src_schema..[[''
        AND     table_name like '']]..src_table..[[''
        ']]

    elseif (conn_db == 'EXASOL') then
        src_system_stmt = [['
        SELECT column_table, column_name
        FROM exa_all_columns
        WHERE column_schema like '']]..src_schema..[[''
        AND column_table like '']]..src_table..[[''
        ']]

    elseif (conn_db == 'DB2') then
        src_system_stmt = [['
        SELECT table_name, column_name
        FROM SYSIBM.COLUMNS
        WHERE table_schema like '']]..src_schema..[[''
        AND table_name like '']]..src_table..[[''
        ']]

    elseif (conn_db == 'MYSQL') then
        src_system_stmt = [['
        select table_name, column_name
        from information_schema.columns
        WHERE table_schema like '']]..src_schema..[[''
        AND table_name like '']]..src_table..[[''
        ']]
        
    elseif (conn_db == 'POSTGRES') then
        src_system_stmt = [['
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema like '']]..src_schema..[[''
        AND table_name like '']]..src_table..[[''
        ']]
    end

    src_system_part = [[ select * from (import from ]]..conn_type..[[ at ]]..conn_name..[[ statement 
        ]]..src_system_stmt..[[ 
        )]]

    return src_system_part
end
-----------------------------------------------------------------------------------------
function get_delta_load_information(wrapper, conn_type, conn_db, conn_name, src_schema, src_table, delta_column)

-- todo: quote parameters for the DB
src_cols_stmt = get_src_cols_stmt(conn_type, conn_db, conn_name, src_schema, src_table)


-- use snapshot mode to avoid rollbacks
info_tbl = wrapper:query_values([[/*snapshot execution*/
        with constr as (
        select constraint_schema, constraint_table,    COLUMN_NAME
        from EXA_ALL_CONSTRAINT_COLUMNS
        where constraint_schema = :tgt_schema and
            CONSTRAINT_TABLE = :tgt_table and
            CONSTRAINT_TYPE = 'PRIMARY KEY'
        order by 1,    2),
    src_cols (src_tbl_name, src_col_name) as 
    (]]..src_cols_stmt..[[),
    cols as (
        select t.column_schema, t.column_table, t.column_name, t.column_ordinal_position, 
		CASE WHEN t.COLUMN_TYPE like 'HASHTYPE%' 
		     THEN 'RAWTOHEX("'|| t.column_name||'")'
		     ELSE '"' || t.COLUMN_NAME || '"'
		END as column_name_src
        from EXA_ALL_COLUMNS t
				left join src_cols s
				on s.src_tbl_name = t.column_table
        		and s.src_col_name = t.column_name
        where t.column_schema = :tgt_schema
        and t.column_table = :tgt_table
		and (not ]]..tostring(cross_check_column_list)..[[ or s.src_col_name is not null)
        ),
    no_key_cols as (
        select cols.column_schema, cols.column_table, cols.column_name
        from cols minus
        select constr.constraint_schema, constr.constraint_table, constr.column_name
        from constr    ),
    delta_cols as (
        select column_schema, column_table, column_name, column_type
        from EXA_ALL_COLUMNS 
        where column_name = :delta_column
        and column_schema = :tgt_schema
        and column_table = :tgt_table
                    ),
    all_tables as (
        select *
        from EXA_ALL_TABLES
        WHERE TABLE_SCHEMA = :tgt_schema and
              TABLE_NAME = :tgt_table    ),
    tables_with_collist as (
        
        select cols.column_schema schema_name, cols.column_table table_name,
            case when count(constr.column_name) = 0 
                then NULL
                else group_concat(distinct 'a.' || '"' || constr.column_name || '"' || '=b.' || '"' || constr.column_name || '"' SEPARATOR ' and ') end key_columns,
            group_concat(distinct '"' || cols.column_name || '"' order by    column_ordinal_position    ) all_columns,
			group_concat(distinct cols.column_name_src order by	column_ordinal_position	) all_src_columns,
            group_concat(distinct 'a.' || '"' || no_key_cols.column_name || '"' || '=b.' || '"' || no_key_cols.column_name || '"') no_key_columns
        from cols
            left join constr on
                constr.constraint_schema = cols.column_schema and
                constr.constraint_table = cols.column_table
            left join no_key_cols on
                no_key_cols.column_schema = cols.column_schema and
                no_key_cols.column_table = cols.column_table
        group by 1,2)
select
    '"' || all_tables.table_schema || '"' table_schema,
    '"' || all_tables.table_name || '"' table_name,
    all_columns,
    all_src_columns,
    key_columns,
    no_key_columns,
    case when length(delta_cols.column_name) > 0 then 
        '"' || delta_cols.column_name || '"'
        else NULL end delta_name,
    delta_cols.column_type delta_type
from all_tables
        left join tables_with_collist on
        all_tables.table_schema = tables_with_collist.schema_name and
        all_tables.table_name = tables_with_collist.table_name
    left join delta_cols on
        delta_cols.column_schema = tables_with_collist.schema_name and
        delta_cols.column_table = tables_with_collist.table_name
order by 1,2 asc;
]])

return info_tbl
-- table_schema, table_name, all_columns(unified), all_src_columns, key_columns, key_columns, no_key_columns, delta_name, delta_type

end
-----------------------------------------------------------------------------------------
-- get max value for a column on exasol side
-- special treatment of column if column type is timestamp or date
function get_max_delta(wrapper, schema, tbl, col, col_type)
    wrapper:set_param('max_schema',schema)
    wrapper:set_param('max_tbl',tbl)
    wrapper:set_param('max_col',col)

    max_query = [[max(::max_col)]]

    if(col_type == 'TIMESTAMP' or col_type == 'DATE') then
        max_query = [[to_char(max(::max_col), 'YYYY-MM-DD HH24:MI:SS.FF6')]]
    end

    suc, max_value = wrapper:query([[select ]]..max_query..[[ from ::max_schema.::max_tbl;]])    
    if (max_value == null) then 
        return null
    else
        return max_value[1][1]
    end
end
-----------------------------------------------------------------------------------------
-- returns statement that can be used in the where clause of the source system to filter on a certain column
-- timestamps and dates have a different syntax in each database system.
-- therefore, for each source database different code is generated
function get_max_stmt_for_src(value, value_type, conn_db)
    value_converted = [['']]..value..[['']]
    if (value_type == 'TIMESTAMP' or value_type == 'DATE') then
        if (conn_db == 'MYSQL') then
            value_converted = [[STR_TO_DATE('']]..value..[['', ''%Y-%m-%d %H:%i:%s.%f'')]]

        elseif  (conn_db == 'SQLSERVER') then
            value_converted = [[CONVERT(datetime,'']]..string.sub(value, 1, #value-3)..[['', 121)]]

        elseif  (conn_db == 'DB2') then
            value_converted = [[to_date('']]..value..[['','YYYY-MM-DD HH24.MI.SS.FF6')]]

        elseif  (conn_db == 'ORACLE' or conn_db == 'POSTGRES') then
            value_converted = [[to_timestamp('']]..value..[['', ''YYYY-MM-DD HH24:MI:SS.FF6'')]]
            -- alternatively, use the line underneath
            --value_converted = [[to_date(('']]..value..[['', ''YYYY-MM-DD HH24:MI:SS'')]]

        else -- case: conn_db == 'EXASOL'
            value_converted = [[to_timestamp('']]..value..[['', ''YYYY-MM-DD HH24:MI:SS.FF6'')]]
        end

    end
    return value_converted
end
-----------------------------------------------------------------------------------------
--------------------------- actual script -----------------------------------------------
-----------------------------------------------------------------------------------------

conn_name             = quote(conn_name)
staging_schema         = quote(staging_schema)
conn_db                = string.upper(conn_db)

-- a parameter to enforce that only columns existing both in source and target are loaded
-- if you know that table definitions are the same, you can keep it as false
cross_check_column_list = false

local result_table = {}

-- create schema and tables for log data
job_log_table= 'JOB_LOG'
job_details_table = 'JOB_DETAILS'
create_log_tables(staging_schema,job_log_table,job_details_table)

-- import query wrapper and tell it logging table names
import('ETL.QUERY_WRAPPER','QW')
wrapper = QW.new( staging_schema..'.'..job_log_table, staging_schema..'.'..job_details_table, 'delta_import_on_primary_keys')

-- setup all parameters
wrapper:set_param('conn_type', conn_type)
wrapper:set_param('conn_name', conn_name)
wrapper:set_param('staging_schema',staging_schema)
wrapper:set_param('src_schema', src_schema)
wrapper:set_param('tgt_schema',tgt_schema)
wrapper:set_param('src_table',src_table)
wrapper:set_param('tgt_table',tgt_table)
wrapper:set_param('delta_column',delta_detection_column)



-- pre-check: Does the target table exist?
suc, res = wrapper:query([[select 'table exists' from EXA_ALL_TABLES
    where table_schema = :tgt_schema
    and table_name = :tgt_table ]], {logging=false} )
if(#res == 0) then
    -- no results --> no table
    return finish_with_error('Target does not exist: Table "'.. tgt_schema .. '"."'.. tgt_table ..'" not found.')
end


info = get_delta_load_information(wrapper, conn_type, conn_db, conn_name, src_schema, src_table, delta_detection_column)

-- for versions prior to 6.0.15 and 6.1.3:
-- the rollback is needed to prevent a transaction rollback that might otherwise be caused by 
-- a read-write conflict on the target table
-- therefore, uncomment the following line:
-- wrapper:rollback()

stmts_tbl = {}

-- only one row as result but for loop because of the way the query wrapper works
for tbl_schema, tbl_name, all_columns, all_src_columns, key_columns_stmt, no_key_columns_stmt, delta_name, delta_type in info do

src_schema = quote(src_schema)
tgt_schema = quote(tgt_schema)
src_table  = quote(src_table)
tgt_table  = quote(tgt_table)

    if(all_columns == NULL) then
           -- no columns --> something is wrong
           return finish_with_error('No columns found for table '.. tgt_schema .. '.'.. tgt_table)
    end

    -- create text for statements used later on
    truncate_stmt            = [[truncate table ]]..tgt_schema..[[.]]..tgt_table..[[;]]
    create_staging_stmt      = [[create or replace table ]]..staging_schema..[[.]]..tgt_table..[[ like ]]..tgt_schema..[[.]]..tgt_table..[[;]]
    drop_staging_stmt        = [[drop table ]]..staging_schema..[[.]]..tgt_table..[[;]]

    select_star_src_stmt     = [[select ]]..changeQuotesForDb(conn_db, all_src_columns) ..[[ from ]]..quoteForDb(conn_db,src_schema)..[[.]]..quoteForDb(conn_db, src_table)
    if (conn_db == 'POSTGRES') then
        -- PostgreSQL converts all table column names into lowercase, unless quoted. Thus, the select to be sent to POSTGRES needs to be prepared accordingly.
        select_star_src_stmt=string.lower(select_star_src_stmt)
    end
    full_load_tgt_stmt       = [[import into ]]..tgt_schema..[[.]]..tgt_table..[[ (]]..all_columns..[[) from ]]..conn_type..[[ at ]]..conn_name..[[ statement ']]..select_star_src_stmt..[[';]]
    full_load_staging_stmt   = [[import into ]]..staging_schema..[[.]]..tgt_table..[[ (]]..all_columns..[[) from ]]..conn_type..[[ at ]]..conn_name..[[ statement ']]..select_star_src_stmt..[[';]]

    if (key_columns_stmt == NULL) then
        -- no key_column --> truncate & full load
        table.insert(stmts_tbl, {src_schema, tgt_schema, tgt_table, 'Full load (No Primary Key)', truncate_stmt})
        table.insert(stmts_tbl, {'', '', '', '', full_load_tgt_stmt})
        
        else
                table.insert(stmts_tbl, {'', '', '', '', create_staging_stmt})
                merge_stmt     = [[merge into ]]..tgt_schema..[[.]]..tgt_table..[[ a using ]]..staging_schema..[[.]]..tgt_table..[[ b on ]]..key_columns_stmt..
                                                [[ when matched then update set ]]..no_key_columns_stmt..
                                                [[ when not matched then insert (]]..all_columns..[[) values (]]..all_columns..[[);]]
                                                
                
                
                if (delta_name == NULL) then
                     -- we don't have a delta detection column
                     --  --> full load into staging table & merge
             table.insert(stmts_tbl, {src_schema, tgt_schema, tgt_table, 'Full load and merge (No delta_detection column)', full_load_staging_stmt})
        
        else
            -- we have a delta column
            max_delta     = get_max_delta(wrapper, tgt_schema, tgt_table, delta_name, delta_type)
            if (max_delta == NULL) then
                 -- we have a delta column and a primary key, but no values with max_delta
                 -- --> full load into staging table & merge
                 table.insert(stmts_tbl, {src_schema, tgt_schema, tgt_table, 'Full load and merge (Only null values in delta_detection column)', full_load_staging_stmt})
            else    
                 -- we have a delta column and a primary key, and a max_delta
                 -- --> load into staging table with where clause & merge
                 max_delta_src_format  = get_max_stmt_for_src(max_delta, delta_type, conn_db)
                 select_delta_src_stmt = [[select ]].. changeQuotesForDb(conn_db, all_src_columns) ..[[ from ]]..quoteForDb(conn_db,src_schema)..[[.]]..quoteForDb(conn_db,src_table)..[[ where ]]..quoteForDb(conn_db,delta_name)..[[ >= ]]..max_delta_src_format
                 if (conn_db == 'POSTGRES') then
                     -- PostgreSQL converts all table column names into lowercase, unless quoted. Thus, the select to be sent to POSTGRES needs to be prepared accordingly.
                     select_delta_src_stmt = string.lower(select_delta_src_stmt)
                 end
                 delta_load_stmt       = [[import into ]]..staging_schema..[[.]]..tgt_table..[[ (]]..all_columns..[[) from ]]..conn_type..[[ at ]]..conn_name..[[ statement ']]..select_delta_src_stmt..[[';]]
                 table.insert(stmts_tbl, {src_schema, tgt_schema, tgt_table, 'Delta load starting at '.. max_delta, delta_load_stmt})        
            end         
        end
        
                -- whenever a primary key is present
        table.insert(stmts_tbl, {'', '', '', '', merge_stmt})
        table.insert(stmts_tbl, {'', '', '', '', drop_staging_stmt})
        
        end

end -- end (for-loop)

if (execute_statements) then
    for i = 1, #stmts_tbl do    
        wrapper:query(stmts_tbl[i][5])
    end
    return wrapper:finish()
else
    wrapper:finish()
end


-- setup output and exit

length_src_schema         = getMaxLengthForColumn(stmts_tbl,1)
length_tgt_schema         = getMaxLengthForColumn(stmts_tbl,2)
length_tbl                = getMaxLengthForColumn(stmts_tbl,3)
length_actions            = getMaxLengthForColumn(stmts_tbl,4)
length_stmts              = getMaxLengthForColumn(stmts_tbl,5)


exit(stmts_tbl, 
    "source_schema VARCHAR("..length_src_schema..
    "), target_schema VARCHAR("..length_tgt_schema..
    "), table_name VARCHAR("..length_tbl..
    "), planned_action VARCHAR("..length_actions..
    "), stmts VARCHAR("..length_stmts..")")


/



execute script database_migration.delta_import(
'JDBC',              -- conn_type (JDBC / ORA/ EXA/...) To load from another schema in the same DB: set conn_type to 'LOCAL', conn_db to '' and conn_name to ''
'MYSQL',             -- conn_db (ORACLE, MYSQL, SQLSERVER, POSTGRES, EXASOL)
'JDBC_MYSQL',        -- conn_name, case sensitive
'vw_mysql',          -- source_schema_name, case sensitive
'delta_mysql',       -- source_table case sensitive
'MySqlDelta',        -- target_schema_name, case sensitive
'DELTA_MYSQL',       -- target_table case sensitive
'last_updated',      -- delta_detection_column case sensitive
'DELTA_STAGING',     -- staging_schema, used for log files and temporary data storage
false                -- execute_statements: boolean. set to true to actually run the statements. false only generates preview of what would be done
);
