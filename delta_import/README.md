## Delta import
The delta-import script syncs tables from a source system to an Exasol database. It's made for use on a regular basis as only new or updated rows will be loaded from source to target.


This script loads only the new rows from the source system. To merge updated/new rows into the destination, primary keys are used. Therefore, primary keys must be set in Exasol. The script works also with primary keys that are in a deactivated state in Exasol.

An update column has to be defined for the script. This column is used to determine new columns and therefore has to contain a value that increases for new/changed values, e.g. an increasing job number or a timestamp.

#### Prerequesites
To use this script you need:
- A source system (The script was tested on 'Oracle' and 'MySQL')
- A target table that:
    - has the same structure as the source table ( see [the import scripts](https://github.com/EXASOL/database-migration) for further instructions)
    - has a primary key
    - has a column which contains a value that is incremented for new/changed entries (e.g. a 'last updated' timestamp or an increasing job number)

#### Installation
- Download/ copy the [Query Wrapper](https://github.com/EXASOL/etl-utils/blob/master/query_wrapper.sql) from the ETL-Utils repository into EXAplus. The Query Wrapper is a library used for logging. Execute the create script statement. This will generate the *query_wrapper* script inside the schema 'ETL'.

- Download/ copy the script [delta_import_on_primary_keys.sql](delta_import_on_primary_keys.sql) from above into EXAplus. Change the following line inside the script to point towards the *query_wrapper*-script if necessary.
```sql
import('ETL.QUERY_WRAPPER','QW')
```

- Once you're done with your changes, execute the create script statement.


#### Script input parameters
- **conn_type:** type of connection used to connect to the source system. Can be one of the connection types the import statement supports -> e.g. JDBC (to use with a generic database), EXA, ORA
- **conn_db:** Type of source system database. Currently supports 'MYSQL', 'ORACLE' or 'EXASOL'
- **conn_name:** connection name -> that connection has to be created upfront and is used for the import statements
- **source_schema_name:** Name of the schema that contains the *source* table(s)
- **target_schema_name:** Name of the schema that contains the *target* table(s)
- **table_name:** Name of the target table(s), can contain wildcards
- **delta_detection_column:** Name of the column in the target table that should be used for delta loading. The maximum value of this column is searched in the target table and only values greater than this value will be loaded from the source system. This could e.g. be an increasing job number in the source system or a 'last modified' timestamp.
- **staging_schema:**  Name of a temporary schema used for storing data from the source system before merging them into the target table

#### Script output
Besides updating the target table, the script generates additioinal information about the loading process in two logging tables. They can be found inside the staging_schema.
- __JOB_LOG:__ Contains information about the executed jobs, like execution time and status.
- __JOB_DETAILS:__ Contains details on the loading process and the executed SQL statements. Use the following statement to see only entries that contain a log message:
```SQL
SELECT * FROM <staging_schema>.JOB_DETAILS
WHERE LOG_LEVEL = 'LOG';
```

## Example

#### Table in target schema

Table `EMPLOYEES` has a primary key on `ID`

| ID | FIRST_NAME | LAST_NAME | TELEPHONE_NR | LAST_UPDATED |
|----|------------|-----------|--------------|--------------|
| 1  | John       | Green     | 0123-45      | 2015-08-15   |
| 2  | Lucy       | Smith     | 0123-46      | 2017-07-01   |
| 3  | Paul       | Taylor    | 0123-47      | 2017-08-01   |



#### Table in source system:
Data has changed since the last update of the target tables.
Row 2 has been modified, row 4 and 5 have been added.

| ID | FIRST_NAME | LAST_NAME | TELEPHONE_NR | LAST_UPDATED |
|----|------------|-----------|--------------|--------------|
| 1  | John       | Green     | 0123-45      | 2015-08-15   |
| 2  | Lucy       | Smith     | **0987-65**  | **2018-02-01**     |
| 3  | Paul       | Taylor    | 0123-47      | 2017-08-01         |
| 4  | **Emma**   | **Williams** | **0123-48**   | **2018-01-15** |
| 5  | **Jacob**  | **Brown** | **0123-49**      | **2018-02-01** |



#### Performing the delta load:
- Use column `LAST_UPDATED` as delta detection column:

```sql
execute script database_migration.delta_import(
'JDBC',  		   -- conn_type (JDBC / EXA/...)
'MYSQL', 		   -- conn_db (ORACLE, MYSQL)
'JDBC_MYSQL',     -- conn_name
'MY_SOURCE_SCHEMA',	-- source_schema_name
'MY_TARGET_SCHEMA',  -- target_schema_name
'EMPLOYEES',     		-- table_name (can contain wildcards)
'LAST_UPDATED',    		-- delta_detection_column
'DELTA_STAGING' -- staging_schema_name
);
```

- The highest value in column `LAST_UPDATED` is *2017-08-01* --> Therefore only rows with a more recent update date will be considered
- Rows with `ID` 4 and 5 will be added to the existing table
- Row with `ID` 2 will be updated
