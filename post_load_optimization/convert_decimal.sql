/*


--------------------------------------------------------------------------------------------
	UPDATED VERSION OF THIS SCRIPT AVAILABLE
	--> have a look at convert_datatypes which 
	offers the same conversions as this script but even more
--------------------------------------------------------------------------------------------



  short:
     This script creates datatype optimization suggestions for you. You can run this after
     importing your data. Selecting smaller datatypes might improve performance.
  long:
     Value ranges of numeric datatypes might be different between databases.
     For ease of use the database migration scripts might select larger datatypes
     compared to what is really required. The following script will generate some
     suggestions for you for datatype optimizations. You can then review them and
     execute them. Therefore please just copy the script output (lots of SQL
     commands) into a new editor and execute them.
     You'll probably benefit in terms of performance selecting matching datatypes.
 */

create schema if not exists database_migration; --in case of EXASOL v5: 

--first parameter: SCHEMA name or SCHEMA_FILTER (can be %)
--second parameter: TABLE name or TABLE_FILTER  (can be %)	
create or replace script database_migration.convert_decimal(schema_name, table_name) 
 as
res = query([[select
			column_schema,
			column_table,
			column_name,
			COLUMN_NUM_PREC,COLUMN_NUM_SCALE
		from
			exa_all_columns
		where
			column_type_id = 3 and --type_id of DECIMAL
			column_schema like :schema_filter and -- e.g. '%' to convert all schema
			COLUMN_TABLE like :table_filter	--e.g. '%' to convert all tables
			and column_OBJECT_TYPE='TABLE'
			and COLUMN_NUM_SCALE = 0 -- for instance only handle values without scale
			and COLUMN_NUM_PREC >9
]],{schema_filter=schema_name, table_filter=table_name})
for i=1,#res do 
		scm = quote(res[i][1])
		tbl = quote(res[i][2])
		col = quote(res[i][3])
		dColumns = query([[select
				count(::col_name),1
			from
				::curr_schema.::curr_table				
		union all
			select
				max(length(abs(::col_name))),2
			from
				::curr_schema.::curr_table
		 order by 2 asc]], {curr_schema=scm, curr_table=tbl,col_name=col});
		if dColumns[1][1]==0 then
			--no rows in table -> do nothing
			output('--KEEP (EMPTY): '..quote(res[i][1])..'.'..quote(res[i][2])..' '..quote(res[i][3]))
		elseif dColumns[2][1]<=9 and res[i][4] > 9 then
			--fits into 32Bit
			--rows in table but seems to be date only (without time information)
			--currRowCount = query([[alter table ::curr_schema.::curr_table MODIFY (::col_name DATE)]],
			--				{curr_schema=res[i][1], curr_table=res[i][2], col_name=res[i][3]})
			--output('CHANGE TO DATE: '..res[i][1]..'.'..res[i][2]..' '..res[i][3])
			output('alter table '..quote(res[i][1])..'.'..quote(res[i][2])..' MODIFY '..quote(res[i][3])..' DECIMAL(9); --from DEC('..res[i][4]..') max length: '..dColumns[2][1])
		elseif (dColumns[2][1]<=18 and res[i][4] > 18) then
			--fits into 64Bit
			output('alter table '..quote(res[i][1])..'.'..quote(res[i][2])..' MODIFY '..quote(res[i][3])..' DECIMAL(18);  --from DEC('..res[i][4]..') max length: '..dColumns[2][1])
--			output('test')
		else
			output('--KEEP (DECIMAL): '..(res[i][1])..'.'..(res[i][2])..' '..quote(res[i][3])..' orig: DEC('..res[i][4]..') max length: '..dColumns[2][1])
		end
	--query('commit')
	end
/
commit;

execute script database_migration.convert_decimal('TEST_SCHEMA', 'MY_FACT_TABLE') WITH OUTPUT;
execute script database_migration.convert_decimal('TEST_SCHEMA', '%') WITH OUTPUT;
