## Delta import
This script can be used to perform a delta import to load updated or new rows. To merge updated/new rows into the destination primary keys are used. Therefore, primary keys must be set in Exasol. The script works also with primary_keys that are in a deactivated state in Exasol.

An update column has to be defined for the script. This column is used to determine new columns and therefore has to contain a value that increases for new values, e.g. an increasing job number or a timestamp.

## Example
### Table in source system:

| Id | First Name | Last Name | Department | Start_Date |
|----|------------|-----------|------------|------------|
| 1  | John       | Green     | Marketing  | 2015-08-15 |
| 2  | Lucy       | Smith     | Sales      | 2017-07-01 |
| 3  | Paul       | Taylor    | Sales      | 2017-08-01 |
| 4  | Emma       | Williams  | Marketing  | 2018-01-15 |
| 5  | Jacob      | Brown     | Sales      | 2018-02-01 |



### Tables in target schema

Table __employees__: Has no primary key

| First Name | Last Name | Department | Start_Date |
|------------|-----------|------------|------------|
| John       | Green     | Marketing  | 2015-08-15 |
| Lucy       | Smith     | Sales      | 2017-07-01 |
| Paul       | Taylor    | Sales      | 2017-08-01 |


Table __employees_with_pk__: Has a primary key on *id*

| Id | First Name | Last Name | Department | Start_Date |
|----|------------|-----------|------------|------------|
| 1  | John       | Green     | Marketing  | 2015-08-15 |
| 2  | Lucy       | Smith     | Sales      | 2017-07-01 |
| 3  | Paul       | Taylor    | Sales      | 2017-08-01 |


Execute delta loading for both tables with column *Start_Date* as update column:

```sql
execute script database_migration.delta_import(
'JDBC',  		-- conn_type (JDBC / EXA/...)
'MYSQL', 		-- conn_db (ORACLE, MYSQL)
'JDBC_MYSQL', 	-- conn_name
'MY_SCHEMA_%',  -- schema_name (can contain wildcards)
'%',     		-- table_name (can contain wildcards)
'Start_Date',    		-- update_column
'DELTA_STAGING' -- staging_schema_name
);
```
Table __employees__ will be truncated, all 5 rows will be loaded once again.

For table __employees_with_pk__ only the rows with id 4 and 5 will be loaded

