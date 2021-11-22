create schema if not exists database_migration;
/* 
	This script will generate create schema, create table and import statements 
	to load all needed data from an Oracle database. Automatic datatype conversion is
	applied whenever needed. Feel free to adjust it. 
	Primary and foreign key constraints will be generated but can be commented out via script parameters.
	This script can also create and populate check tables to identify differences.
	A summary table for all the check tables allows for easy queriying.
*/

;
--/
create or replace script database_migration.ORACLE_TO_EXASOL (
CONNECTION_NAME				 -- name of the database connection inside exasol -> e.g. mysql_db
,IDENTIFIER_CASE_INSENSITIVE -- TRUE if identifiers should be put uppercase
,SCHEMA_FILTER               -- filter for the schemas to generate and load, e.g. 'my_schema', 'my%', 'schema1, schema2', '%'
,TABLE_FILTER                -- filter for the tables to generate and load, e.g. 'my_table', 'my%', 'table1, table2', '%'
,PARALLEL_STATEMENTS  		 -- number or parallel statements for imports using balanced bin packing with partitions, if partitions are available, else ora_hash partitioning of the rowid will be used.
,CREATE_PK					 -- TRUE if primary keys should be created, else they will generated but commented out
,CREATE_FK					 -- TRUE if foreign keys should be created, else they will generated but commented out. NOTE that FKs on tables that do not exist/were not migrated, will fail.
,CHECK_MIGRATION			 -- TRUE if checking tables and summary should be created
) RETURNS TABLE 
AS

-- Functions
function string.startsWith(String,word)
   return string.sub(String,1,string.len(word))==word
end


-- checking the parameter types
if(type(IDENTIFIER_CASE_INSENSITIVE) ~= 'boolean') then 
	error('expected a boolean value for parameter IDENTIFIER_CASE_INSENSITIVE, but instead got ' .. type(IDENTIFIER_CASE_INSENSITIVE) .. '.')
end

if(type(CREATE_PK) ~= 'boolean') then 
	error('expected a boolean value for parameter CREATE_PK, but instead got ' .. type(CREATE_PK) .. '.')
end

if(type(CREATE_FK) ~= 'boolean') then 
	error('expected a boolean value for parameter CREATE_FK, but instead got ' .. type(CREATE_FK) .. '.')
end

ps = 1
if(type(PARALLEL_STATEMENTS) == 'number') then 
	ps = math.floor(PARALLEL_STATEMENTS)
else
	error('expected an interger value for parameter PARALLEL_STATEMENTS, but instead got ' .. type(PARALLEL_STATEMENTS) .. '.')
end

if(type(CHECK_MIGRATION) ~= 'boolean') then 
	error('expected a boolean value for parameter CHECK_MIGRATION, but instead got ' .. type(CHECK_MIGRATION) .. '.')
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

-- check whether connection is OCI or JDBC Connection
CONNECTION_TYPE = get_connection_type(CONNECTION_NAME)


-- set schema and table filter conditions
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




-- initialize variables for parallel statement
sql_ora_part_bin = [[]]
t_part = {}
s_sn = nil
s_tn = nil
t_bin_stat = {}
t_sql_bin_values = {}

i_bin_idx = nil
n_bin_sum = 0

-- if statements should run in parallel
if(ps > 1) then
	-- generate the sql statement to count the number of rows per partition
	success, sql_ora_part_res = pquery([[
	select 	'select ''''' || table_owner || ''''' SN, ''''' || table_name || ''''' TN, ''''' || partition_name || ''''' PN , count(*) cnt from "' || table_owner || '"."' || table_name || '" partition ("' || partition_name || '")' 
					|| case when rownum != count(*) over() then ' union all ' end SQL_PART_CNT
	from 	(
			import from ]] .. CONNECTION_TYPE .. [[ at ]] .. CONNECTION_NAME .. [[ statement
			'
			select 	table_owner, table_name, partition_name 
			from 	all_tab_partitions
			where 	table_owner ]] .. SCHEMA_STR .. [[ 
			and		table_name ]] .. TABLE_STR .. [[
			'
	)
	]])
	--output(sql_ora_part_res.statement_text)
	if(not success) then
		error(sql_ora_part_res.error_message)
	end
	
	-- if there are partitions at all
	if(#sql_ora_part_res >= 1) then 
	
		-- generate the import statement to count the number of rows per partition
		sql_ora_part_res2 = [[import into (SN varchar(128), TN varchar(128), PN varchar(128), CNT decimal(36,0)) from ]] .. CONNECTION_TYPE .. [[ at ]] .. CONNECTION_NAME .. [[ statement ']]
		for i=1, #sql_ora_part_res do
			sql_ora_part_res2 = sql_ora_part_res2 .. sql_ora_part_res[i].SQL_PART_CNT
		end
		sql_ora_part_res2 = sql_ora_part_res2 .. [[']]
		
		-- execute the import statement to count the number of rows per partition. Empty partitions are filtered out.
		-- NOTE: do not remove the order by clause, as it is necessary.
		success, ora_part_res3 = pquery([[
		select	SN, TN, PN, CNT
		from 	(
				]] .. sql_ora_part_res2 ..  [[
		)
		where CNT > 0
		order by SN, TN, CNT desc
		]])
		--output(ora_part_res3.statement_text)
		if(not success) then
			error(ora_part_res3.error_message)
		end
		
		-- populate the lua table from the userdata
		for i=1, #ora_part_res3 do
			t_part[#t_part +1] = {
				['SN'] = ora_part_res3[i].SN,
				['TN'] = ora_part_res3[i].TN,
				['PN'] = ora_part_res3[i].PN,
				['CNT'] = tonumber(ora_part_res3[i].CNT)
			}
		end
		
		-- iterate over all partitions and decide to which bin / statement they should be assigned to
		for i=1, #t_part do
		
			-- start a new cycle and set / reset variables if a schema name or a table name is nill (initial loop) or changes
			if(s_sn == nil or t_part[i]['SN'] ~= s_sn or s_tn == nil or t_part[i]['TN'] ~= s_tn) then 
				s_sn = t_part[i]['SN']
				s_tn = t_part[i]['TN']
				t_bin_stat = {}
				n_bin_sum = 0				
			end
			
			i_bin_idx = 1
			--iterate over all bins, and save the one with the least sum of rows
			for bin=1, ps do
				-- if a bin is not set yet, initialize it, store the bin index and break the loop
				if(t_bin_stat[bin] == nil) then 
					i_bin_idx = bin
					t_bin_stat[bin] = {['PN'] = {}, ['SUM'] = 0}
					break
				-- search for the new minimal bin count and set save the bin index
				elseif(t_bin_stat[bin]['SUM'] <= n_bin_sum) then 
					i_bin_idx = bin
					n_bin_sum = t_bin_stat[bin]['SUM']
				end
			end
			
			-- update the the bin stats and partition assignment per statement for the current schema/table
			t_bin_stat[i_bin_idx]['SUM'] = t_bin_stat[i_bin_idx]['SUM'] + t_part[i]['CNT']
			t_bin_stat[i_bin_idx]['PN'][#t_bin_stat[i_bin_idx]['PN'] +1] = t_part[i]['PN']
			
			-- add the bin column to the partition table, the updated sum is the value to compare other bin counts with in the next iteration
			t_part[i]['BIN'] = i_bin_idx
			n_bin_sum = t_bin_stat[i_bin_idx]['SUM']
			
			-- this will form the basis for a "from values" clause
			t_sql_bin_values[#t_sql_bin_values +1] = [[(']] .. 
				t_part[i]['SN'] .. [[', ']] .. 
				t_part[i]['TN'] .. [[', ']] .. 
				t_part[i]['PN'] .. [[', ]] .. 
				t_part[i]['CNT'] .. [[, ]] .. 
				t_part[i]['BIN'] .. 
			[[)]]
				
		end
		
		sql_ora_part_bin = [[select * from values ]] .. table.concat(t_sql_bin_values, ', ') .. [[ as t(sn, tn, pn, cnt, bin_nr)]]
		
	else 
		sql_ora_part_bin = [[select cast(null as varchar(128)) sn, cast(null as varchar(128)) tn, cast(null as varchar(128)) pn, cast(null as int) cnt, cast(null as int) bin_nr from dual]]
	end
	
else 
	sql_ora_part_bin = [[select cast(null as varchar(128)) sn, cast(null as varchar(128)) tn, cast(null as varchar(128)) pn, cast(null as int) cnt, cast(null as int) bin_nr from dual]]
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


success, res = pquery([[
with ora_cols as ( 
        select  * 
        from    (
		import from ]]..CONNECTION_TYPE..[[ at ::c
		statement 
		        '
		        select	]]..all_tab_cols..[[  
		        from	all_tab_columns 
		        where 	table_name in (
                				select  table_name 
                                from    all_tables 
                                where   owner ]]..SCHEMA_STR..[[ 
                                and     table_name ]]..TABLE_STR..[[
                )
		        and		owner ]]..SCHEMA_STR..[[ 
		        and 	table_name ]]..TABLE_STR..[[ 
		        and     owner || ''.'' || table_name NOT IN (
                				select 	owner || ''.'' || view_name 
                                from 	all_views
		                        )
		        '
        )
)

, ora_base as (
        SELECT  EXA_SCHEMA_NAME, 
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
        FROM    ora_cols
)

, ora_cons_pk as (
        select  pk.*, count(*) over(partition by owner, table_name) cnt_pk
        from    (
                import from ]]..CONNECTION_TYPE..[[ at ::c
				statement 
                        '
                    	select  acc.owner, acc.table_name, acc.column_name, acc.position as pos, ac.status, acc.constraint_name,
                                ]] .. exa_upper_begin .. [[acc.owner]]          .. exa_upper_end .. [[ as EXA_SCHEMA_NAME,
                                ]] .. exa_upper_begin .. [[acc.table_name]]     .. exa_upper_end .. [[ as EXA_TABLE_NAME,
                                ]] .. exa_upper_begin .. [[acc.column_name]]    .. exa_upper_end .. [[ as EXA_COLUMN_NAME
                        from    all_tables ta
                        join    all_cons_columns acc 
                        on      ta.owner ]]..SCHEMA_STR..[[ 
                        and     ta.table_name ]]..TABLE_STR..[[
                        and     ta.owner = acc.owner
                        and     ta.table_name = acc.table_name
                        join    all_constraints ac 
                        on      acc.owner = ac.owner
                        and     acc.table_name = ac.table_name
                        and     acc.constraint_name = ac.constraint_name
                        and     ac.constraint_type = ''P''
                        ' 
        ) pk
)

, ora_cons_fk as (
        select  *
        from    (
                import from ]]..CONNECTION_TYPE..[[ at ::c
				statement 
                        '
                        select  acc.owner, acc.table_name, acc.column_name, acc.position as pos, ac.status, acc.constraint_name,
                                acc_r.owner as r_owner, acc_r.table_name as r_table_name, acc_r.column_name as r_column_name,
                                ]] .. exa_upper_begin .. [[acc.owner]]          .. exa_upper_end .. [[ as EXA_SCHEMA_NAME,
                                ]] .. exa_upper_begin .. [[acc.table_name]]     .. exa_upper_end .. [[ as EXA_TABLE_NAME,
                                ]] .. exa_upper_begin .. [[acc.column_name]]    .. exa_upper_end .. [[ as EXA_COLUMN_NAME,
                                ]] .. exa_upper_begin .. [[acc_r.owner]]        .. exa_upper_end .. [[ as EXA_REFERENCED_SCHEMA_NAME,
                                ]] .. exa_upper_begin .. [[acc_r.table_name]]   .. exa_upper_end .. [[ as EXA_REFERENCED_TABLE_NAME,
                                ]] .. exa_upper_begin .. [[acc_r.column_name]]  .. exa_upper_end .. [[ as EXA_REFERENCED_COLUMN_NAME
                        from    all_tables ta
                        join    all_cons_columns acc 
                        on      ta.owner ]]..SCHEMA_STR..[[ 
                        and     ta.table_name ]]..TABLE_STR..[[
                        and     ta.owner = acc.owner
                        and     ta.table_name = acc.table_name
                        join    all_constraints ac 
                        on      acc.owner = ac.owner
                        and     acc.table_name = ac.table_name
                        and     acc.constraint_name = ac.constraint_name
                        and     ac.constraint_type = ''R''
                        join    all_cons_columns acc_r
                        on      ac.r_owner = acc_r.owner
                        and     ac.r_constraint_name = acc_r.constraint_name
                        and     acc.position = acc_r.position
                        '
        )
)

, nls_format as (
        select * 
        from    (
				import from ]]..CONNECTION_TYPE..[[ at ::c 
                statement 
                        'select * 
                        from    nls_database_parameters 
                        where   parameter in (''NLS_TIMESTAMP_FORMAT'',''NLS_DATE_FORMAT'',''NLS_DATE_LANGUAGE'',''NLS_CHARACTERSET'', ''NLS_NCHAR_CHARACTERSET'')
                        '
        )
)

, cr_schema as (
        with EXA_SCHEMAS as (
                select  distinct EXA_SCHEMA_NAME as EXA_SCHEMA 
                from    ora_base 
        )
        select  'create schema if not exists "' ||  EXA_SCHEMA || '";' as cr_schema 
        from    EXA_SCHEMAS
)

, cr_tables as (
        select  'create or replace table "' || EXA_SCHEMA_NAME || '"."' || EXA_TABLE_NAME || '" (' || cols || '); ' || cols2 || '' as tbls 
        from    (select EXA_SCHEMA_NAME, EXA_TABLE_NAME, 
                        group_concat( 
                            	case 
                                        when data_type in ('CHAR', 'NCHAR') then 																		'"' || EXA_COLUMN_NAME || '"' || ' ' || 'char(' || char_length || ')'
                                        when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then 														'"' || EXA_COLUMN_NAME || '"' || ' ' || 'varchar(' || char_length || ')'
                                        when data_type in ('CLOB', 'NCLOB') then																		'"' || EXA_COLUMN_NAME || '"' || ' ' || 'varchar(2000000)'
                                        when data_type = 'XMLTYPE' then 																				'"' || EXA_COLUMN_NAME || '"' || ' ' || 'varchar(2000000)'
										when data_type = 'RAW' 
												then 																									'"' || EXA_COLUMN_NAME || '"' || ' ' ||
														case 	when data_length <= 1024 then 																'hashtype(' || data_length || ' byte)'
																else 																						'varchar(' || (data_length * 2) || ') ascii'
														end
                                        when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) 
                                                then 																									'"' || EXA_COLUMN_NAME || '"' || ' ' ||  
                                                        case    when data_scale > 36 then 																	'decimal(' || 36 || ',' || 36 || ')' 
                                                                when data_precision > 36 and data_scale <= 36 then 											'decimal(' || 36 || ',' || data_scale || ')' 
                                                                when data_precision <= 36 and data_scale > data_precision then  							'decimal(' || data_scale || ',' || data_scale || ')' 
                                                                else 																						'decimal(' || data_precision || ',' || data_scale || ')' 
                                                        end
                                        when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) 
                                                then 																									'"' || EXA_COLUMN_NAME || '"' || ' ' ||  
                                                        case    when data_scale > 36 then 																	'decimal(' || 36 || ',' || 36 || ')' 
                                                                when data_precision > 36 and data_scale <= 36 then 											'decimal(' || 36 || ',' || data_scale || ')' 
                                                                when data_precision <= 36 and data_scale > data_precision then  							'decimal(' || data_scale || ',' || data_scale || ')' 
                                                                else 																						'decimal(' || data_precision || ',' || data_scale || ')' 
                                                        end
                                        when data_type = 'NUMBER' and (	data_length is not null and 
																		data_precision is null and 
																		data_scale is not null) then 													'"' || EXA_COLUMN_NAME || '"' || ' ' || 'integer' 
                                        when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then 								'"' || EXA_COLUMN_NAME || '"' || ' ' || 'double precision'
                                        when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then 							'"' || EXA_COLUMN_NAME || '"' || ' ' || 'double precision'
                                        when data_type = 'DATE' then 																					'"' || EXA_COLUMN_NAME || '"' || ' ' || 'timestamp'
                                        when data_type like 'TIMESTAMP(%)%' or data_type like 'TIMESTAMP%' then											'"' || EXA_COLUMN_NAME || '"' || ' ' || 'timestamp'
                                        when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then 															'"' || EXA_COLUMN_NAME || '"' || ' ' || 'timestamp' 
                                        when data_type like 'INTERVAL YEAR%TO MONTH%' then 																'"' || EXA_COLUMN_NAME || '"' || ' ' || 'interval year(' || case when data_precision = 0 then 1 else data_precision end || ') to month'
                                        when data_type like 'INTERVAL DAY%TO SECOND%' then 																'"' || EXA_COLUMN_NAME || '"' || ' ' || 'interval day(' || case when data_precision = 0 then 1 else data_precision end || ') to second(' || data_scale || ')'
                                        when data_type = 'BOOLEAN' then 																				'"' || EXA_COLUMN_NAME || '"' || ' ' || 'boolean'
                                        -- Fallback for unsupported data types
                                        -- else '"' || EXA_COLUMN_NAME || '"' || ' ' ||  'varchar(2000000) /* UNSUPPORTED DATA TYPE : ' || data_type
                                end || 
                                case    when 	identity_column='YES' and 
												data_type = 'NUMBER' and 
												data_precision is not null and 
												data_scale is not null then 																			' identity' 
								end || 
                                case when nullable= 'N' then 																							' not null' 
									 else ''
								end

                                order by column_id 
                                SEPARATOR ', '
                        ) as cols,
                        group_concat( 
                                case    when data_type not in ( 
													'CHAR', 'NCHAR', 'VARCHAR', 'VARCHAR2', 'NVARCHAR2', 'CLOB', 'NCLOB', 'XMLTYPE', 
                                                    'DECIMAL', 'NUMBER', 'DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE', 
                                                    'DATE', 'BOOLEAN', 'TIMESTAMP', 'RAW') 
                                        		and data_type not like 'TIMESTAMP(%)%' 
                                            	and data_type not like 'TIMESTAMP%WITH%TIME%ZONE%' 
												and data_type not like 'INTERVAL YEAR%TO MONTH%' 
												and data_type not like 'INTERVAL DAY%TO SECOND%' 
										then chr(13) || '--UNSUPPORTED DATA TYPE : "' || EXA_COLUMN_NAME || '" ' || data_type
                                end
                        ) as cols2 
                from    ora_base 
                group   by EXA_SCHEMA_NAME, EXA_TABLE_NAME
        )
)

, cr_constraints_pk as (
        select  'alter table "' || EXA_SCHEMA_NAME || '"."' || EXA_TABLE_NAME || '" add primary key (' || listagg('"' || EXA_COLUMN_NAME ||'"', ', ') within group(order by pos) || ') enable;' as EXA_CONSTRAINT
        from    ora_cons_pk
        group   by EXA_SCHEMA_NAME, EXA_TABLE_NAME
)

, cr_constraints_fk as (
        select  'alter table "' || EXA_SCHEMA_NAME || '"."' || EXA_TABLE_NAME || '" add foreign key (' || listagg('"' || EXA_COLUMN_NAME || '"', ', ') within group(order by pos) || ') references "' || EXA_REFERENCED_SCHEMA_NAME || '"."' || EXA_REFERENCED_TABLE_NAME || '"(' || listagg('"' || EXA_REFERENCED_COLUMN_NAME ||'"', ', ') within group(order by pos) || ') enable;' as EXA_CONSTRAINT
        from    ora_cons_fk
        group   by EXA_SCHEMA_NAME, EXA_TABLE_NAME, EXA_REFERENCED_SCHEMA_NAME, EXA_REFERENCED_TABLE_NAME, constraint_name
)

, cr_import_stmts as(
		with cl as (
				select	exa_schema_name, owner, exa_table_name, table_name,
						listagg(
								case 
		                                when data_type in ('CHAR', 'NCHAR') then 																				'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then 																'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type in ('CLOB', 'NCLOB') then 																				'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type = 'XMLTYPE' then 																						'"' || EXA_COLUMN_NAME || '"'
										when data_type = 'RAW' then																								'"' || EXA_COLUMN_NAME || '"'
		                                when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) then 							'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) then 								'"' || EXA_COLUMN_NAME || '"'  
		                                when data_type = 'NUMBER' and (data_length is not null and data_precision is null and data_scale is not null) then 		'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then 										'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then 									'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type = 'DATE' then 																							'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type like 'TIMESTAMP(%)%' or data_type like 'TIMESTAMP' then 													'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then 																	'"' || EXA_COLUMN_NAME || '"'
		                                when data_type like 'INTERVAL YEAR%TO MONTH%' then 																		'"' || EXA_COLUMN_NAME || '"'
		                                when data_type like 'INTERVAL DAY%TO SECOND%' then 																		'"' || EXA_COLUMN_NAME || '"' 
		                                when data_type = 'BOOLEAN' then 																						'"' || EXA_COLUMN_NAME || '"' 
		                                -- else '--UNSUPPORTED DATATYPE IN COLUMN ' || COLUMN_NAME || ' Oracle Datatype: ' || data_type 
                        		end
								, ', '
						) within group(order by column_id) exa_col_list,
						
						listagg(
		                		case 
		                                when data_type in ('CHAR', 'NCHAR') then 																				'"' || column_name || '"' 
		                                when data_type in ('VARCHAR','VARCHAR2', 'NVARCHAR2') then 																'"' || column_name || '"' 
		                                when data_type = 'CLOB' then 																							'"' || column_name || '"'
										when data_type = 'NCLOB' then 																							'to_clob("' || column_name || '")' 
		                                when data_type = 'XMLTYPE' then 																						'"' || column_name || '"'
										when data_type = 'RAW' then																								'rawtohex("' || column_name || '")'
		                                when data_type in ('DECIMAL') and (data_precision is not null and data_scale is not null) then 							'"' || column_name || '"' 
		                                when data_type = 'NUMBER' and (data_precision is not null and data_scale is not null) then 								'"' || column_name || '"'  
		                                when data_type = 'NUMBER' and (data_length is not null and data_precision is null and data_scale is not null) then 		'"' || column_name  || '"'
		                                when data_type = 'NUMBER' and (data_precision is null and data_scale is null) then 										'"' || column_name || '"' 
		                                when data_type in ('DOUBLE PRECISION', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE') then 									'cast("' || column_name || '" as DOUBLE PRECISION)' 
		                                when data_type = 'DATE' then 																							'"' || column_name || '"' 
		                                when data_type like 'TIMESTAMP(%)' or data_type like 'TIMESTAMP' then 													'"' || column_name || '"' 
		                                when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' then 																	'cast("' || column_name || '" at time zone ''''00:00'''' as TIMESTAMP)'
		                                when data_type like 'INTERVAL YEAR%TO MONTH%' then 																		
												case 	when data_precision > 0 then																			'to_char("' || column_name || '")'
														else																									'substr(cast("' || column_name || '" as varchar2(30)), 10, 5)'
												end
		                                when data_type like 'INTERVAL DAY%TO SECOND%' then 																		'to_char("' || column_name || '")'
		                                when data_type = 'BOOLEAN' then '"' || column_name || '"' 
		                                -- else '--UNSUPPORTED DATATYPE IN COLUMN ' || column_name || ' Oracle Datatype: ' || data_type  
		                        end
		                        , ', '
		                )  within group (order by column_id) ora_col_list
						
		        from    ora_base 
		        group   by exa_schema_name, owner, exa_table_name, table_name 
		)
		, ora_bin_part as (
				]] .. sql_ora_part_bin .. [[

		)
		, ora_stmt_part as (
				select	exa_schema_name, owner, exa_table_name, table_name, bin_nr, 
						listagg('select /*+parallel*/ ' || ora_col_list || ' from "' || owner || '"."' || table_name || '"' || case when bin_nr is not null then ' partition("' || pn ||'")' end, ' union all ') stmt
				from 	cl 
				left 	join ora_bin_part bp
				on 		cl.owner = bp.sn
				and 	cl.table_name = bp.tn
				group	by exa_schema_name, owner, exa_table_name, table_name, bin_nr
		)
		, ora_stmt_part_oh as (
				select 	exa_schema_name, owner, exa_table_name, table_name, 
						case when sp.bin_nr is null and ]] .. ps .. [[ > 1 then stmt || ' where ora_hash(rowid, ' || ml || ') = ' || l else stmt end stmt
				from 	ora_stmt_part sp 
				left 	join (select null bin_nr, level -1 l, ]] .. ps .. [[ -1 ml from dual connect by level <= ]] .. ps .. [[) oh 
				on  	sp.bin_nr is null and oh.bin_nr is null
		)
		, ora_stmt_part_oh_agg as (
				select	exa_schema_name, owner, exa_table_name, table_name,
						listagg(' statement ''' || stmt || '''', ' ') stmt_agg
				from 	ora_stmt_part_oh
				group 	by exa_schema_name, owner, exa_table_name, table_name
		)
		select 	'import into "' || e.exa_schema_name || '"."' || e.exa_table_name || '" (' || e.exa_col_list || ') from  ]]..CONNECTION_TYPE..[[ at ]] .. CONNECTION_NAME .. [[ ' || o.stmt_agg || ';' import_stmt
		from 	cl e
		join 	ora_stmt_part_oh_agg o
		on 		e.owner = o.owner
		and 	e.table_name = o.table_name		
)

, check_expr as (
		select 	db_system,	
				exa_schema_name,
				exa_table_name,
				exa_column_name,
				owner,
				table_name,
				case 	when db_system = 'Exasol' then exa_schema_name
						else owner
				end sn, -- schema name
				case 	when db_system = 'Exasol' then exa_table_name
						else table_name
				end tn, -- table name
				case 	when db_system = 'Exasol' then exa_column_name
						else column_name
				end cn, -- column name
		      	case    when db_system = 'Exasol' then '"' || exa_column_name || '"'
		                else '"' || column_name || '"'
		        end qcn, -- quoted column name
			  	case	when db_system = 'Exasol' then '"' || exa_schema_name || '"."' || exa_table_name || '"'
		                else '"' || owner || '"."' || table_name || '"'
		        end qstn, -- quoted schema table name
		        case    when db_system = 'Exasol' then '"' || exa_table_name || '"."' || exa_column_name || '"'
		                else '"' || table_name || '"."' || column_name || '"'
		        end qtcn, -- quoted table column name
	
				column_id,
				data_type,
				case 	when data_type = 'NUMBER' then 	
								case 	when data_precision IS NULL AND data_scale IS NULL then 										'double precision'
				        				when data_precision IS NULL AND data_scale = 0 then 											'decimal(36,0)'
				        				when data_scale > 36 then 																		'decimal(' || 36 || ',' || 36 || ')'
										when data_precision > 36 AND data_scale <= 36 then 												'decimal(' || 36 || ',' || data_scale || ')'
				                    	when data_precision <= 36 AND data_precision >= data_scale then 								'decimal(' || data_precision || ',' || data_scale || ')'
				            	end
		            	when data_type in ('BINARY_DOUBLE', 'BINARY_FLOAT', 'FLOAT') then 												'double precision'
		    	end trg_num_dt,
				nullable,
				
	        	metric_id,
	        	'DATABASE_MIGRATION' as metric_schema_name,
	        	exa_table_name || '_MIG_CHK' as metric_table_name,
	        	
	        	case	when metric_id = 0 and column_id = 1 then																		'cast(count(*) as decimal(36,0))'
	        			when metric_id = 1 and nullable = 'Y' and (
								data_type in ('CHAR', 'NCHAR', 'VARCHAR','VARCHAR2', 'NVARCHAR2', 'RAW', 'DECIMAL', 'NUMBER', 'DATE') or
								data_type like 'TIMESTAMP(%)%' or 
								data_type like 'TIMESTAMP%' or
								data_type like 'TIMESTAMP%WITH%TIME%ZONE%' or
								data_type like 'INTERVAL YEAR%TO MONTH%' or
								data_type like 'INTERVAL DAY%TO SECOND%') then															'cast(sum(case when ' || local.qtcn ||' is null then 1 end) as decimal(36,0))'
	        			when metric_id = 2 and (
								data_type in ('CHAR', 'NCHAR', 'VARCHAR','VARCHAR2', 'NVARCHAR2', 'RAW', 'DECIMAL', 'NUMBER', 'DATE') or
								data_type like 'TIMESTAMP(%)%' or 
								data_type like 'TIMESTAMP%' or
								data_type like 'TIMESTAMP%WITH%TIME%ZONE%' or
								data_type like 'INTERVAL YEAR%TO MONTH%' or
								data_type like 'INTERVAL DAY%TO SECOND%') then															'cast(count(distinct ' || local.qtcn || ') as decimal(36,0))'
	        			
	        			when data_type in ('CHAR', 'VARCHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2') then 
	        					case	when metric_id = 3 then																			'min("C_' || local.cn ||'"."' || local.cn || '_TOP")'
		                        		when metric_id = 4 then																			'min("C_' || local.cn ||'"."' || local.cn || '_OCC")' 
				            			when metric_id = 5 then																			'cast(min(length(' || local.qtcn || ')) as decimal(36,0))'
	        							when metric_id = 6 then																			'cast(avg(length(' || local.qtcn || ')) as double precision)'
	        							when metric_id = 7 then																			'cast(median(length(' || local.qtcn || ')) as decimal(36,0))'
	        							when metric_id = 8 then																			'cast(max(length(' || local.qtcn || ')) as decimal(36,0))'
								end
						
						when data_type in ('BINARY_DOUBLE', 'BINARY_FLOAT', 'FLOAT', 'NUMBER') then 
								case	when metric_id = 3 then																			'cast(min(' || local.qtcn || ') as ' || local.trg_num_dt || ')'
										when metric_id = 4 then																			'cast(avg(' || local.qtcn || ') as ' || local.trg_num_dt || ')'
										when metric_id = 5 then																			'cast(median(' || local.qtcn || ') as ' || local.trg_num_dt || ')'
										when metric_id = 6 then																			'cast(max(' || local.qtcn || ') as ' || local.trg_num_dt || ')'
								end
						
						when data_type = 'DATE' or data_type like 'TIMESTAMP(%)' or (data_type like 'TIMESTAMP%WITH%TIME%ZONE%' and db_system = 'exasol') then 
								case	when metric_id = 3 then																			'cast(min(' || local.qtcn || ') as timestamp)'
										when metric_id = 4 then																			'cast(median(' || local.qtcn || ') as timestamp)'
										when metric_id = 5 then																			'cast(max(' || local.qtcn || ') as timestamp)'
								end
						when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' and db_system = 'oracle' then 
								case	when metric_id = 3 then																			'cast(min(cast(' || local.qtcn || ' at time zone ''00:00'' as timestamp with time zone)) at time zone ''00:00'' as timestamp)'
										when metric_id = 4 then																			'cast(median(cast(' || local.qtcn || ' at time zone ''00:00'' as timestamp with time zone)) at time zone ''00:00'' as timestamp)'
										when metric_id = 5 then																			'cast(max(cast(' || local.qtcn || ' at time zone ''00:00'' as timestamp with time zone)) at time zone ''00:00'' as timestamp)'
								end
								
						when data_type like 'INTERVAL DAY% TO SECOND%' or (data_type like 'INTERVAL YEAR% TO MONTH' and data_precision > 0) then 
								case	when metric_id = 3 then																			'to_char(min(' || local.qtcn || '))'
										when metric_id = 4 then																			'to_char(median(' || local.qtcn || '))'
										when metric_id = 5 then																			'to_char(max(' || local.qtcn || '))'
								end
						when data_type like 'INTERVAL YEAR% TO MONTH' and data_precision = 0 then
								case	when metric_id = 3 then																			'substr(cast(min(' || local.qtcn || ') as varchar2(30)), 10, 5)'
										when metric_id = 4 then																			'substr(cast(median(' || local.qtcn || ') as varchar2(30)), 10, 5)'
										when metric_id = 5 then																			'substr(cast(max(' || local.qtcn || ') as varchar2(30)), 10, 5)'
								end
	        	end metric_column_expression,

	        	case 	when metric_id = 0 and column_id = 1 then																		'"CNT_' || local.tn || '"'
	        			when metric_id = 1 and nullable = 'Y' and (
								data_type in ('CHAR', 'NCHAR', 'VARCHAR','VARCHAR2', 'NVARCHAR2', 'RAW', 'DECIMAL', 'NUMBER', 'DATE') or
								data_type like 'TIMESTAMP(%)%' or 
								data_type like 'TIMESTAMP%' or
								data_type like 'TIMESTAMP%WITH%TIME%ZONE%' or
								data_type like 'INTERVAL YEAR%TO MONTH%' or
								data_type like 'INTERVAL DAY%TO SECOND%') then															'"' || local.cn || '_CNT_NULL"'
	        			when metric_id = 2 and  (
								data_type in ('CHAR', 'NCHAR', 'VARCHAR','VARCHAR2', 'NVARCHAR2', 'RAW', 'DECIMAL', 'NUMBER', 'DATE') or
								data_type like 'TIMESTAMP(%)%' or 
								data_type like 'TIMESTAMP%' or
								data_type like 'TIMESTAMP%WITH%TIME%ZONE%' or
								data_type like 'INTERVAL YEAR%TO MONTH%' or
								data_type like 'INTERVAL DAY%TO SECOND%') then															'"' || local.cn || '_CNT_DST"'
	        			
	        			when data_type in ('CHAR', 'VARCHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2') then 
	        					case	when metric_id = 3 then																			'"' || local.cn || '_TOP"' 
		                        		when metric_id = 4 then																			'"' || local.cn || '_OCC"'
				            			when metric_id = 5 then																			'"' || local.cn || '_MIN"'
	        							when metric_id = 6 then																			'"' || local.cn || '_AVG"'
	        							when metric_id = 7 then																			'"' || local.cn || '_MED"'
	        							when metric_id = 8 then																			'"' || local.cn || '_MAX"'
								end
						
						when data_type in ('BINARY_DOUBLE', 'BINARY_FLOAT', 'FLOAT', 'NUMBER') then 
								case	when metric_id = 3 then																			'"' || local.cn || '_MIN"'
										when metric_id = 4 then																			'"' || local.cn || '_AVG"'
										when metric_id = 5 then																			'"' || local.cn || '_MED"'
										when metric_id = 6 then																			'"' || local.cn || '_MAX"'
								end
						
						when data_type = 'DATE' or data_type like 'TIMESTAMP(%)' or (data_type like 'TIMESTAMP%WITH%TIME%ZONE%' and db_system = 'exasol') then 
								case	when metric_id = 3 then																			'"' || local.cn || '_MIN"'
										when metric_id = 4 then																			'"' || local.cn || '_MED"'
										when metric_id = 5 then																			'"' || local.cn || '_MAX"'
								end
						when data_type like 'TIMESTAMP%WITH%TIME%ZONE%' and db_system = 'oracle' then 
								case	when metric_id = 3 then																			'"' || local.cn || '_MIN"'
										when metric_id = 4 then																			'"' || local.cn || '_MED"'
										when metric_id = 5 then																			'"' || local.cn || '_MAX"'
								end
								
						when data_type like 'INTERVAL DAY% TO SECOND%' or (data_type like 'INTERVAL YEAR% TO MONTH' and data_precision > 0) then 
								case	when metric_id = 3 then																			'"' || local.cn || '_MIN"'
										when metric_id = 4 then																			'"' || local.cn || '_MED"'
										when metric_id = 5 then																			'"' || local.cn || '_MAX"'
								end
						when data_type like 'INTERVAL YEAR% TO MONTH' and data_precision = 0 then
								case	when metric_id = 3 then																			'"' || local.cn || '_MIN"'
										when metric_id = 4 then																			'"' || local.cn || '_MED"'
										when metric_id = 5 then																			'"' || local.cn || '_MAX"'
								end
	        	end metric_column_name,
	        	case 	when data_type in ('CHAR', 'VARCHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2')  and metric_id = 1 then
		                		'(select substr(listagg(' || local.qtcn || ', ' || case when db_system = 'Exasol' then ''',''' else ''''',''''' end || ') within group(order by ' || local.qtcn || '), 1, 2000) as "' || local.cn || '_TOP", cast(min(cnt) as decimal(36,0)) as "' || local.cn || '_OCC" ' ||
						        'from (' ||
		                			'select ' || local.qtcn || ', count(*) cnt, max(count(*)) over() max_cnt ' ||
		                			'from ' || local.qstn || ' ' || 
		                			'group by ' || local.qtcn || 
	                			') "' || local.tn || '" ' ||
		                        'where cnt = max_cnt) "C_' || local.cn || '"' 
	        	end metric_column_subselect
		from	ora_base
		, 		(select 'Oracle' db_system union all select 'Exasol' db_system)
		,		(select level -1 metric_id from dual connect by level <= 9)
		where 	local.metric_column_expression is not null
		and		]] .. tostring(CHECK_MIGRATION) .. [[
)

, cr_check_table as (
	select  db_system, exa_schema_name, exa_table_name, metric_table_name,
			listagg(metric_column_subselect, ', ') within group(order by column_id, metric_id) sql_subselect,
			'create or replace table "' || exa_schema_name || '"."' || metric_table_name || '" as ' ||
	        'select cast(''' || db_system || ''' as varchar2(20)) as "DB_SYSTEM", ' || 
	        listagg(metric_column_expression || ' as ' || metric_column_name, ', ') within group(order by column_id, metric_id) || 
	        ' from ' || qstn || case when local.sql_subselect is not null then ', ' || local.sql_subselect end || ';' as sql_text
	from 	check_expr
	where 	db_system = 'Exasol'
	group by db_system, qstn, exa_schema_name, exa_table_name, owner, table_name, metric_table_name

)

, ins_check_table as (
	select  listagg(metric_column_subselect, ', ') within group(order by column_id, metric_id) sql_subselect,
			'insert into "' || exa_schema_name || '"."' || metric_table_name || '" ' ||
			'select * from (import from ]]..CONNECTION_TYPE..[[ at ]] .. CONNECTION_NAME .. [[ statement '''  ||
	        'select cast(''''' || db_system || ''''' as varchar2(20)) as "DB_SYSTEM", ' || 
	        listagg(metric_column_expression || ' as ' || metric_column_name, ', ') within group(order by column_id, metric_id) || 
	        ' from ' || qstn || case when local.sql_subselect is not null then ', ' || local.sql_subselect end ||
	        case when db_system != 'Exasol' then ''');' end as sql_text
	from 	check_expr
	where 	db_system != 'Exasol'
	group by db_system, qstn, exa_schema_name, exa_table_name, owner, table_name, metric_table_name
)

, cr_check_summary as (
    select 	1 ord2, 'create or replace table "' || metric_schema_name || '"."' || exa_schema_name || '_MIG_CHK" (schema_name varchar(128), table_name varchar(128), column_name varchar(128), metric_schema varchar(128), metric_table varchar(128),  metric_name varchar(128), exasol_metric varchar(2000), oracle_metric varchar(2000), check_timestamp timestamp default current_timestamp);' as sql_text
    from 	check_expr
    group by metric_schema_name, exa_schema_name
    union all
    select 2 ord2, 'insert into "' || metric_schema_name || '"."' || exa_schema_name || '_MIG_CHK" (schema_name, table_name, column_name, metric_schema, metric_table, metric_name, exasol_metric, oracle_metric) ' 
            || listagg(sql_text, '') within group(order by case when db_system = 'Exasol' then 1 else 2 end) || ' '
            || 'select e.schema_name, e.table_name, e.column_name, e.metric_schema, e.metric_table, e.metric_name, e.exasol_metric, o.oracle_metric from exasol e join oracle o on e.schema_name = o.schema_name and e.table_name = o.table_name and e.metric_name = o.metric_name; ' as sql_text
    from (
    
            select  db_system, exa_schema_name, exa_table_name, metric_schema_name,
                    case when db_system = 'Exasol' then 'with ' else ', ' end || db_system || ' as ( '
                    || listagg(
                    	'select ''' || exa_schema_name || ''' as schema_name, ''' || exa_table_name || ''' as table_name,  ''' || exa_column_name || ''' column_name, ''' || metric_schema_name || ''' metric_schema, ''' || metric_table_name || ''' metric_table, ''' || metric_column_name || ''' as metric_name, to_char(' || metric_column_name || ') as ' || db_system || '_metric from "' || exa_schema_name || '"."' || metric_table_name || '" where DB_SYSTEM = ''' || db_system || '''', ' union all ')
                    || ' )'  as sql_text
            from check_expr
            group by db_system, exa_schema_name, exa_table_name, metric_schema_name, metric_table_name
            order by case when db_system = 'Exasol' then 1 else 2 end
    )
    group by exa_schema_name, exa_table_name, metric_schema_name

)
select  sql_text 
from    (
        select 1 as ord_hlp,'-- session parameter values are being taken from Oracle systemwide database_parameters and converted. However these should be confirmed before use.' as sql_text
        union all
        select 2, '-- Oracle DB''s NLS_CHARACTERSET is set to : ' || "VALUE" from nls_format where "PARAMETER"='NLS_CHARACTERSET'
		union all
		select 2.1, '-- Oracle DB''s NLS_NCHAR_CHARACTERSET is set to : ' || "VALUE" from nls_format where "PARAMETER"='NLS_NCHAR_CHARACTERSET'
        union all
        select 3,'-- ALTER SESSION SET NLS_DATE_LANGUAGE=''' || "VALUE" || ''';' from nls_format where "PARAMETER"='NLS_DATE_LANGUAGE'
        union all
        select 4,'-- ALTER SESSION SET NLS_DATE_FORMAT=''' || replace("VALUE",'R','Y') || ''';' from nls_format where "PARAMETER"='NLS_DATE_FORMAT'
        union all
        select 5,'-- ALTER SESSION SET NLS_TIMESTAMP_FORMAT=''' || replace(regexp_replace("VALUE",'XF+','.FF6'),'R','Y') || ''';' from nls_format where "PARAMETER"='NLS_TIMESTAMP_FORMAT'
		union all
		select 6, '-- ALTER SESSION SET NLS_NUMERIC_CHARACTERS=''' || session_value || ''';' from exa_parameters where parameter_name = 'NLS_NUMERIC_CHARACTERS'
        union all
        select 7, a.* from cr_schema a
        union all
        select 8, b.* from cr_tables b where b.TBLS not like '%();%'
        union all
        select 9, import_stmt from cr_import_stmts
		union all
        select 10, case when not ]] .. tostring(CREATE_PK) .. [[ then '-- ' end || EXA_CONSTRAINT from cr_constraints_pk
        union all
        select 11, case when not ]] .. tostring(CREATE_FK) .. [[ then '-- ' end || EXA_CONSTRAINT from cr_constraints_fk
		union all
		select 12, sql_text from cr_check_table
		union all
		select 13, sql_text from ins_check_table
		union all
		select 14, sql_text from cr_check_summary
) 
order by ord_hlp
]],{c=CONNECTION_NAME, s=SCHEMA_FILTER, t=TABLE_FILTER})

--output(res.statement_text)
if not success then error(res.error_message) end
return(res)

/
;



create or replace connection oracle_jdbc	to	'jdbc:oracle:thin:@192.168.56.106:1521/cdb2' user 'C##DB_MIG' identified by 'C##DB_MIG';
create or replace connection oracle_oci		to					  '192.168.56.106:1521/cdb2' user 'C##DB_MIG' identified by 'C##DB_MIG';
import from JDBC at ORACLE_JDBC statement 	'select ''Connection works'' from dual';
import from ORA at ORACLE_OCI statement 	'select ''Connection works'' from dual';


execute script database_migration.oracle_to_exasol(
	'ORACLE_OCI', 	-- connection name
	true, 			-- case insensitivity flag
	'C##DB_MIG', 	-- schema name filter
	'DIM_DATE, DIM_PRODUCT, DIM_STORE, SALES, SALES_POSITION',	-- table name filter
	4, 				-- degree of parallelism for the import statements.
	false, 			-- flag for primary key generation.
	false, 			-- flag for foreign key generation.
	false			-- flag for creation and loading of checking tables
)
--with output
;
