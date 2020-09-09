## Delta import
The delta-import script syncs tables from a source system to an Exasol database. It's made for use on a regular basis as only new or updated rows will be loaded from source to target.


This script loads only the new rows from the source system. To merge these updated/new rows into the destination, primary keys are used. Therefore, primary keys must be set in Exasol. The script works also with primary keys that are set in a deactivated state in Exasol.

An update column has to be defined for the script. This column is used to determine new columns and therefore has to contain a value that increases for new/changed values, e.g. an increasing job number or a timestamp.

#### Prerequesites
To use this script you need:
- A source system (The script was tested on 'Oracle', 'SQL Server' and 'MySQL')
- A target table that:
    - has the same structure as the source table ( see [the import scripts](https://github.com/EXASOL/database-migration) for further instructions)
    - has a primary key
    - has a column which contains a value that is incremented for new/changed entries (e.g. a 'last updated' timestamp or an increasing job number)
- The query wrapper (see next step)

#### Installation
- Download/ copy the [Query Wrapper](https://github.com/EXASOL/etl-utils/blob/master/query_wrapper.sql) from the ETL-Utils repository into your SQL editor. The Query Wrapper is a library used for logging. Execute the `create script` statement. This will generate a script called *query_wrapper* inside the schema 'ETL'.

- Download/ copy the script [delta_import_on_primary_keys.sql](delta_import_on_primary_keys.sql) from above into your SQL editor. Change the following line inside the script to point towards the *query_wrapper*-script if necessary.
```sql
import('ETL.QUERY_WRAPPER','QW')
```

- Once you're done with your changes, execute the `create script` statement for the script `delta_import`.


#### Script input parameters
- **`conn_type`**
Type of connection used to connect to the source system. Can be one of the connection types the import statement supports -> e.g. JDBC (to use with a generic database), EXA, ORA
- **`conn_db`**
Type of source system database. Currently supports 'MYSQL', 'ORACLE' 'SQLSERVER' or 'EXASOL'
- **`conn_name`**
Connection name -> the connection has to be created upfront and is used for the import statements
- **`source_schema_name`**
Name of the schema that contains the *source* table (case-sensitive)
- **`source_table_name`**
Name of the table in the *source* schema (case-sensitive)
- **`target_schema_name`**
Name of the schema that contains the *target* table (case-sensitive)
- **`target_table_name`**
Name of the table in the *target* schema (case-sensitive)
- **`delta_detection_column`**
Name of the column in the target table that should be used for delta loading. The maximum value of this column in the target table will be determined and only values greater than this value will be loaded from the source system. This could e.g. be an increasing job number in the source system or a 'last modified' timestamp. (case-sensitive)
- **`staging_schema`**
Name of a temporary schema used for storing data from the source system before merging them into the target table. It's recommended to create a dedicated schema for this.
- **`execute_statement`**
Boolean, set to `true` if you want to actually run the statements. `false` generates only a preview of what would be done without performing the delta load. Mode `false` can be used to check whether everything is set up correctly, if so you can change it to true.




#### Script output
The script output depends on the `execute_statement` value that you pass as an input. Test your setup with `execute_statement` first to check whether the primary keys are used and you specified the delta_detection_column properly.

Besides generating output and updating the target table, the script generates additioinal information about the loading process in two logging tables. They can be found inside the staging_schema.
- __JOB_LOG:__ Contains information about the executed jobs, like execution time and status.
- __JOB_DETAILS:__ Contains details on the loading process and the executed SQL statements. Use the following statement to e.g. see only entries that contain a log message:
```SQL
SELECT * FROM <staging_schema>.JOB_DETAILS
WHERE LOG_LEVEL = 'LOG';
```

## Example

#### Table in target schema (Exasol), schema `IMPORTANT_DATA`:

Table `EMPLOYEES` has a primary key on `ID`

| ID | FIRST_NAME | LAST_NAME | TELEPHONE_NR | LAST_UPDATED |
|----|------------|-----------|--------------|--------------|
| 1  | John       | Green     | 0123-45      | 2015-08-15 08:00:00.000  |
| 2  | Lucy       | Smith     | 0123-46      | 2017-07-01 10:00:00.000  |
| 3  | Paul       | Taylor    | 0123-47      | 2017-08-01 09:00:00.000  |



#### Table in source system (Mysql), schema `MASTER_DATA`:
Data has changed since the last update of the target tables.
Row 2 has been modified, row 4 and 5 have been added.

| ID | FIRST_NAME | LAST_NAME | TELEPHONE_NR | LAST_UPDATED             |
|----|------------|-----------|--------------|--------------------------|
| 1  | John       | Green     | 0123-45      | 2015-08-15 08:00:00.000  |
| 2  | Lucy       | Smith     | 0987-65      | 2018-02-01  11:00:00.000 |
| 3  | Paul       | Taylor    | 0123-47      | 2017-08-01 09:00:00.000  |
| 4  | Emma       | Williams  | 0123-48      | 2018-01-15 09:00:00.000  |
| 5  | Jacob      | Brown     | 0123-49      | 2018-02-01 09:00:00.000  |



#### Performing the delta load:
We will use column `LAST_UPDATED` as delta detection column. First, we will take a look at what the script would do if we would execute it. So we set the last parameter `execute_statement` to **false**.

```sql
execute script database_migration.delta_import(
'JDBC',            -- conn_type (JDBC / EXA/...)
'MYSQL',           -- conn_db (ORACLE, MYSQL)
'CONN_MYSQL',      -- conn_name
'MASTER_DATA',-- source_schema_name
'IMPORTANT_DATA',-- target_schema_name
'EMPLOYEES',-- source_table_name
'EMPLOYEES',       -- target_table_name
'LAST_UPDATED',    -- delta_detection_column
'DELTA_STAGING',   -- staging_schema_name
false               -- execute_statement
);
```
Now that we've seen that it would corretly do a delta load, we set `execute_statement` to **true**.
```sql
execute script database_migration.delta_import(
'JDBC',            -- conn_type (JDBC / EXA/...)
'MYSQL',           -- conn_db (ORACLE, MYSQL)
'CONN_MYSQL',      -- conn_name
'MASTER_DATA',-- source_schema_name
'IMPORTANT_DATA',-- target_schema_name
'EMPLOYEES',-- source_table_name
'EMPLOYEES',       -- target_table_name
'LAST_UPDATED',    -- delta_detection_column
'DELTA_STAGING',   -- staging_schema_name
true               -- execute_statement
);
```

- The highest value in column `LAST_UPDATED` is *2017-08-01 09:00:00.000* --> Therefore only rows with a more recent update date will be considered
- Rows with `ID` 4 and 5 will be added to the existing table
- Row with `ID` 2 will be updated

#### Result in target schema (Exasol), schema `IMPORTANT_DATA`:
| ID | FIRST_NAME | LAST_NAME | TELEPHONE_NR | LAST_UPDATED |
|----|------------|-----------|--------------|--------------|
| 1  | John       | Green     | 0123-45      | 2015-08-15 08:00:00.000  |
| 2  | Lucy       | Smith     | **0987-65**  | **2018-02-01  11:00:00.000**   |
| 3  | Paul       | Taylor    | 0123-47      | 2017-08-01 09:00:00.000        |
| 4  | **Emma**   | **Williams** | **0123-48**   | **2018-01-15 09:00:00.000** |
| 5  | **Jacob**  | **Brown** | **0123-49**      | **2018-02-01 09:00:00.000** |
