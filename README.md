# Database migration
[![Build Status](https://travis-ci.org/exasol/database-migration.svg?branch=master)](https://travis-ci.org/exasol/database-migration)

> ## ⚠️ Please note
>
> This is an **open source project** and is **not officially supported by Exasol**. We are happy to help
> wherever we can, but — since this is not an official Exasol product — **we cannot give any guarantees**.

## Table of Contents
1. [Overview](#overview)
2. [Migration source:](#migration-source)
    * [Azure Blob Storage](#azure-blob-storage)
    * [CSV](#csv)
    * [DB2](#db2)
    * [Exasol](#exasol)
    * [Google BigQuery](#google-bigquery)
    * [MariaDB](#mariadb)
    * [MySQL](#mysql)
    * [Netezza](#netezza)
    * [Oracle](#oracle)
    * [PostgreSQL](#postgresql)
    * [Redshift](#redshift)
    * [S3](#s3)
    * [SAP Hana](#sap-hana)
    * [Snowflake](#snowflake)
    * [SQL Server](#sql-server)
    * [Teradata](#teradata)
    * [Vectorwise](#vectorwise)
    * [Vertica](#vertica)
    
3. [Post-load optimization](#post-load-optimization)
4. [Delta import](#delta-import)


## Overview

This project contains SQL scripts for automatically importing data from various data management systems into Exasol.

You'll find SQL scripts which you can execute on Exasol to load data from certain databases or
database management systems. The scripts try to extract the meta data from the source system and create the appropriate IMPORT statements automatically so that you don't have to care about table names, column names and types.

If you want to optimize existing scripts or create new scripts for additional systems, we would be very glad if you share your work with the Exasol user community.

## Migration source

### Azure Blob Storage

The script [azure_blob_storage_to_exasol.sql](azure_blob_storage_to_exasol.sql) looks different than the other import scripts. It's made to load data from Azure Blob Storage in parallel and needs some preparation before you can use it. See [our documentation](https://docs.exasol.com/loading_data/loading_data_from_amazon_s3_in_parallel.htm) for detailed instructions.
If you just want to import a single file, see 'Import from [CSV](#csv)'.


### CSV

The method of importing a CSV file depends on the location of the file.
- Import a file stored on your **local machine** via EXAplus:
```sql
IMPORT INTO <table> FROM LOCAL CSV FILE '<filename>' <options>;
```
Example:
``` sql
IMPORT INTO MY_SCHEMA.MY_TABLE
FROM LOCAL CSV FILE 'C:\Users\my_user\Downloads\data.csv'
COLUMN SEPARATOR = ',' 
COLUMN DELIMITER = '"' 
ROW SEPARATOR = 'CRLF' -- CR when file was generated on a unix systems, CRLF when created on windows
SKIP = 1 -- skip the header
;
```

- Import from **HDFS**: See [Hadoop ETL UDFs](https://github.com/EXASOL/hadoop-etl-udfs/blob/main/README.md)

- Import from **S3**: See [Load Data from Amazon S3 Using IMPORT](https://docs.exasol.com/db/latest/loading_data/load_data_amazon_s3.htm) for single file import, for importing multiple files scroll down to [S3](#s3)

For more details on `IMPORT` see [IMPORT](https://docs.exasol.com/db/latest/sql/import.htm). For further help on typical CSV-formatting issues, see

* [How to load bad CSV files](https://exasol.my.site.com/s/article/How-to-load-bad-CSV-files?language=en_US)
* [Proper csv export from MySQL](https://exasol.my.site.com/s/article/Proper-csv-export-from-MySQL?language=en_US)
* [Proper csv export from IBM DB2](https://exasol.my.site.com/s/article/Proper-csv-export-from-IBM-DB2?language=en_US)
* [Proper csv export from Oracle](https://exasol.my.site.com/s/article/Proper-csv-export-from-Oracle?language=en_US)
* [Proper csv export from PostgreSQL](https://exasol.my.site.com/s/article/Proper-csv-export-from-PostgreSQL?language=en_US)
* [Proper csv export from Microsoft SQL Server](https://exasol.my.site.com/s/article/Proper-csv-export-from-Microsoft-SQL-Server?language=en_US)

### DB2

See script [db2_to_exasol.sql](db2_to_exasol.sql)

### Exasol

The [exasol_to_exasol.sql](exasol_to_exasol.sql) script generates the statements to migrate one Exasol
database to another. It runs on the **target**, reads the **source** metadata through a connection (EXA or
JDBC) and **returns** the statements to recreate and load the source. It changes nothing itself — you review
the output and run it, in the order returned.

**Step by step**
* **Install** the script on the **target** database (run [exasol_to_exasol.sql](exasol_to_exasol.sql) once;
  it creates `DATABASE_MIGRATION.EXASOL_TO_EXASOL`).
* **Create a connection** on the target pointing at the **source** Exasol database. Both the native **EXA**
  and the **JDBC** interface are built into Exasol — no driver to install (unlike every other source).
  **Prefer EXA**: `IMPORT FROM EXA` is always parallelized, so loading directly from another Exasol database
  is significantly faster. For self-signed certificates add the certificate fingerprint or `nocertcheck` and
  list all source nodes. Ready-to-edit `CREATE CONNECTION` examples and a connection test are at the bottom
  of the script (a self-managed Exasol — on-prem or in any cloud, e.g. AWS/GCP/Azure — and Exasol SaaS, which
  uses a slightly different connection string).
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it (a few seconds, depending on the
  number of tables).
* **Copy the result set** into another session and execute the statements **in the output order**.

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.EXASOL_TO_EXASOL(
   'EXASOL_EXA'  -- CONNECTION_NAME: the connection to the SOURCE database
  ,'EXA'         -- CONNECTION_SETTING: 'EXA' (native, parallel, faster) or 'JDBC'
  ,false         -- IDENTIFIER_CASE_INSENSITIVE: false = verbatim/quoted, recommended (preserves lower/MixedCase); true = fold ALL identifiers to UPPER
  ,'%TPCDS_1GB%' -- SCHEMA_FILTER: schema name/filter, '%' = all (SYS, EXA_STATISTICS and virtual schemas are always excluded)
  ,'%'           -- TABLE_FILTER: table name/filter, '%' = all
  ,true          -- GENERATE_VIEWS: true/false, include views (emitted as CREATE OR REPLACE FORCE VIEW)
  ,'%'           -- VIEW_FILTER: view name/filter, '%' = all
  ,'DISABLE'     -- PK_SETTING: 'DISABLE' (faster load; appends an ENABLE-keys section) or 'ENABLE'
  ,'8'           -- TARGET_VERSION: '8' (default) or '7' (downgrade: TIMESTAMP(p) -> TIMESTAMP)
);
```

This script generates, in this order:
* `CREATE SCHEMA` and `CREATE TABLE` — columns keep their **exact source type** (so every data type *and* its
  character set `ASCII`/`UTF8` is reproduced 1:1), plus `NOT NULL`, `IDENTITY` and column `DEFAULT`s
* primary keys (quoted, in constraint order) and `ALTER TABLE … ADD … FOREIGN KEY`
* `ALTER TABLE … PARTITION BY` and `ALTER TABLE … DISTRIBUTE BY`
* table & column `COMMENT`s
* `IMPORT` of the data (typed transfer — differing source/target NLS does not affect the data; nanosecond
  `TIMESTAMP(9)` is preserved over both EXA and JDBC)
* when `PK_SETTING='DISABLE'`: an **ENABLE PRIMARY & FOREIGN KEYS** section to run after the import (loading
  is much faster with keys disabled; primary keys are enabled before foreign keys)
* views, including their comment, created `WITH FORCE`

System schemas (`SYS`, `EXA_STATISTICS`) and **virtual** objects are skipped. `7.1 → 8` and `7.1 → 7.1`
work out of the box; for a downgrade `8 → 7.1` set `TARGET_VERSION='7'`. Not migrated (out of scope):
functions, scripts/UDFs/adapters, users/roles/privileges, connections.

**Privileges/visibility:** the source metadata is read from the `EXA_ALL_*` system views **through the
connection's user**, so the script sees — and generates statements for — only the objects that user may
access on the source; the generated statements run on the target only where you have the matching
privileges. **To migrate everything, use a user with DBA privileges on both the source and the target.**

See the header of [exasol_to_exasol.sql](exasol_to_exasol.sql) for more information!

### Google BigQuery

In order to connect Exasol to Google BigQuery you need to carry out the steps outlined in [Connecting Google BigQuery to Exasol](https://docs.exasol.com/loading_data/connect_databases/google_bigquery.htm). 

Now, test the connectivity with a simple query:

```sql
SELECT *
FROM   (
               IMPORT FROM JDBC AT <name_of_connection>
			   STATEMENT 'SELECT  1'
	   );
```

For the actual data-migration, see script [bigquery_to_exasol.sql](bigquery_to_exasol.sql)

Note: Due to the lack of an alternative datatype, the following Google BigQuery datatypes; `DATE`,`DATETIME`,`TIMESTAMP` and `ARRAY` are stored as VARCHAR. 

### MariaDB

**Download Driver**

Download the JDBC driver for MariaDB from the [MariaDB connectors page](https://mariadb.com/downloads/connectors/connectors-data-access/java8-connector/). In the Product dropdown menu select **Java 8+connector**, in the Version dropdown menu select **the newest version**, in the OS dropdown menu select **Platform Independent**.

**Configure the Driver in EXAoperation, database versions prior to v8**

Do the following to configure the driver in EXAoperation:
1.	Log in to EXAoperation user interface as an Administrator user.
2.	Select **Configuration > Software** and click the **JDBC Drivers** tab.
3.	Click **Add** to add the JDBC driver details.
4.	Enter the following details for the JDBC properties:
* **Driver Name:** `MariaDB`
* **Main Class:** `org.mariadb.jdbc.Driver`
* **Prefix:** `jdbc:mariadb:`
* **Disable Security Manager:** `Check the box to disable the security manager.` This allows the JDBC Driver to access certificate and additional information.
* **Comment:** `This is an optional field.`

5.	Click **Add** to save the settings.
6.	Select the radio button next to the driver from list of JDBC driver.
7.	Click **Choose File** to locate the downloaded driver and click **Upload** to upload the JDBC driver.

For Exasol v8 or newer use the values above in [Load data using JDBC (generic)](https://docs.exasol.com/db/latest/loading_data/connect_sources/import_data_using_jdbc.htm).

You can find a detailed information about the MariaDB driver at the following link: https://mariadb.com/kb/en/about-mariadb-connector-j/


**Test Connectivity**


To test the connectivity of Exasol to MariaDB create the following connection in your SQL client:

```SQL
CREATE OR REPLACE CONNECTION JDBC_MARIADB
    TO 'jdbc:mariadb://192.168.56.103:3306/my_database'
    USER 'user_name'
    IDENTIFIED BY 'my_password';
```

You need to have CREATE CONNECTION privilege granted to the user used to do this. The connection string and authentication details in the example must be replaced with your own values.

Run the following statement to test the connection:

```SQL
select * from 
(
import from JDBC at JDBC_MARIADB
statement 'select ''Connection works'' from dual'
);
```

For the actual data-migration, see script [mariadb_to_exasol.sql](mariadb_to_exasol.sql)


### MySQL

Create a connection:
```SQL
CREATE CONNECTION <name_of_connection>
TO 'jdbc:mysql://192.168.137.5:3306'
USER '<user>'
IDENTIFIED BY '<password>';
```

Test your connection:
```SQL
SELECT * FROM
(
IMPORT FROM JDBC AT <name_of_connection>
STATEMENT 'SELECT 42 FROM DUAL'
);
```
Then you're ready to use the migration script: [mysql_to_exasol.sql](mysql_to_exasol.sql)

### Netezza

The first thing you need to do is add the IBM Netezza JDBC driver to Exasol. Since Netezza has run out of support in June 2019, the JDBC-driver (`nzjdbc3.jar`) can no longer be found on the official JDBC Download-page of IBM. Anyhow, the driver can be found within your Netezza distribution under following path: 

`nz/kit.version_number/sbin/nzjdbc3.jar   (eg. nz/kit.7.2.1.0/sbin/nzjdbc3.jar)`

In database versions prior to v8, in order to add the driver to Exasol log into your EXAoperation, select the 'Software'-, then 'JDBC Drivers'-Tab.

Click Add, then specify the following details:

* Driver Name: `Netezza`
* Main Class: `org.netezza.Driver`
* Prefix: `jdbc:netezza:`
* Disable Security Manager: `Check this box`

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. 
Select the Netezza driver by locating the nzjdbc3.jar and upload it. When done the .jar file should be listed in the files column for the IBM Netezza driver.

For Exasol v8 or newer use the values above in [Load data using JDBC (generic)](https://docs.exasol.com/db/latest/loading_data/connect_sources/import_data_using_jdbc.htm).

The standard port for Netezza is `5480`.

The Connection-String should look like the following: 
`"jdbc:netezza://'host_ip':'port'/Database-Name" (User-ID, Password)`
`(e.g. jdbc:netezza://127.0.0.1:5480/SYSTEM,  User-ID: ADMIN, Password: Password)`

To test the connectivity of Exasol to your Netezza Instance create the following connection in your SQL-client:

```SQL
CREATE OR REPLACE CONNECTION <name_of_connection>
        TO 'jdbc:netezza://<host_name>:<port>'
        USER '<netezza_username>'
        IDENTIFIED BY '<netezza_password>';
```

You need to have CREATE CONNECTION privilege granted to the user in order to do this.

Test the connectivity with a simple query like:

```SQL
SELECT *
    FROM   (
               IMPORT FROM JDBC AT netezza_connection
               STATEMENT 'SELECT 1 as "sucessfully_connected" from _v_dual '
           );
```
For the actual data-migration, see script [netezza_to_exasol.sql](netezza_to_exasol.sql)

### Oracle

When importing from Oracle, you have two options. You could import via JDBC or the  native Oracle interface (OCI).
- OCI: Follow [Oracle OCI](https://docs.exasol.com/db/latest/loading_data/connect_sources/oracle.htm#OracleCallInterfaceOCI).

  Create a connection:
  ``` SQL
  CREATE CONNECTION <name_of_connection>
  	TO '192.168.99.100:1521/xe'
    USER '<user>'
    IDENTIFIED BY '<password>';
  ```

- JDBC: Follow [Oracle JDBC](https://docs.exasol.com/db/latest/loading_data/connect_sources/oracle.htm#OracleJDBC).

  Create a connection:
  ```SQL
  CREATE CONNECTION <name_of_connection>
  	TO 'jdbc:oracle:thin:@//192.168.99.100:1521/xe'
  	USER '<user>'
    IDENTIFIED BY '<password>';
  ```

Test your connection:
```SQL
SELECT * FROM
(
IMPORT FROM <conn_type> AT <name_of_connection>
STATEMENT 'SELECT 42 FROM DUAL'
);
```
`<con_type>` is either`JDBC` or `ORA`, depending on your connection

Then you're ready to use the migration script: [oracle_to_exasol.sql](oracle_to_exasol.sql)

### PostgreSQL

See script [postgres_to_exasol.sql](postgres_to_exasol.sql)

### Redshift

See script [redshift_to_exasol.sql](redshift_to_exasol.sql)

### S3

The script [s3_to_exasol.sql](s3_to_exasol.sql) looks different than the other import scripts. It's made to load data from S3 in parallel and needs some preparation before you can use it. See [our documentation](https://docs.exasol.com/loading_data/loading_data_from_amazon_s3_in_parallel.htm) for detailed instructions.
If you just want to import a single file, see 'Import from [CSV](#csv)' above.

### SAP Hana

The first thing you need to do is add the SAP Hana JDBC driver to Exasol. The JDBC driver is located in your local SAP Hana Installation folder:

* eg: (C:\Program Files\sap\ngdbc.jar) on Microsoft Windows platforms
* eg: (/usr/sap/ngdbc.jar) on Linux and UNIX platforms

In database versions prior to v8, in order to add the driver to Exasol log into your EXAoperation, select the 'Software', then 'JDBC Drivers'-Tab.

Click Add then specify the following details:

* Driver Name: `SAP`
* Main Class: `com.sap.db.jdbc.Driver`
* Prefix: `jdbc:sap:`
* Disable Security Manager: `Check this box`

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. Select the SAP Hana driver by locating the ngdbc.jar and upload it.
When done the .jar file should be listed in the files column for the SAP Hana driver.

For Exasol v8 or newer use the values above in [Load data using JDBC (generic)](https://docs.exasol.com/db/latest/loading_data/connect_sources/import_data_using_jdbc.htm).

You can find a detailed information about configuring the SAP Hana driver at the following link:
https://help.sap.com/viewer/52715f71adba4aaeb480d946c742d1f6/2.0.00/en-US/ff15928cf5594d78b841fbbe649f04b4.html

The standard port number format for different instances 3NN15, NN- represents the instance number of HANA system to be used in client tools. (eg. 31015 Instance No 10, 30015 Instance No 00)
In order to find out the instance number of your Hana-System type the following into your console:
/usr/sap/HXE and press Autocomplete by tab. The Instance-Number should be displayed by the number of the HDB(XX)-File (eg. HDB90)
Insert the number into your port number (-> eg. 39015)

The Connection-String should look like the following: "jdbc:sap://'host_ip':'port'/"
(User-ID: SYSTEM, Password: Your password)


To test the connectivity of Exasol to your SAP Instance create the following connection in your SQL-client:

```SQL
CREATE OR REPLACE CONNECTION <name_of_connection>
        TO 'jdbc:sap://<host_name>:<port>'
        USER '<sap_username>'
        IDENTIFIED BY '<sap_password>';
```

You need to have CREATE CONNECTION privilege granted to the user used to do this.

Now, test the connectivity with a simple query like:

```SQL

SELECT *
    FROM   (
               IMPORT FROM JDBC AT <name_of_connection>
               STATEMENT 'SELECT 1 from dummy'
           );
```
For the actual data-migration, see script [sap_hana_to_exasol.sql](sap_hana_to_exasol.sql)

### Snowflake

The first thing you need to do is add the Snowflake JDBC driver to Exasol. The JDBC driver can be downloaded from the [Snowflake website](https://docs.snowflake.com/developer-guide/jdbc/jdbc-download).

In database versions prior to v8, in order to add the driver to Exasol log into your EXAoperation, select the 'Software', then 'JDBC Drivers'-Tab.

Click Add then specify the following details:

* Driver Name: `Snowflake`
* Main Class: `net.snowflake.client.jdbc.SnowflakeDriver`
* Prefix: `jdbc:snowflake:`
* Disable Security Manager: `Check this box`

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. Select the Snowflake driver by locating the corresponding jar and upload it. When done the .jar file should be listed in the files column for the Snowflake driver.

For Exasol v8 or newer follow [Load data from Snowflake](https://docs.exasol.com/db/latest/loading_data/connect_sources/snowflake.htm).

You can find a detailed information about configuring the Snowflake driver at the following link:
https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure

To test the connectivity of Exasol to Snowflake create the following connection in your SQL-client:

```SQL
CREATE OR REPLACE CONNECTION SNOWFLAKE_CONNECTION TO
  'jdbc:snowflake://<myorganization>-<myaccount>.snowflakecomputing.com/?warehouse=<my_compute_wh>&role=<my_role>&CLIENT_SESSION_KEEP_ALIVE=true'
  USER '<sfuser>' IDENTIFIED BY '<sfpwd>';
```

You need to have CREATE CONNECTION privilege granted to the user used to do this. Replace the placeholders including <> with your account information.

Now, test the connectivity with a simple query like:

```SQL

SELECT * FROM 
(
IMPORT FROM JDBC AT SNOWFLAKE_CONNECTION
STATEMENT 'select ''Connection works!'' as connection_status'
);
```
For the actual data-migration, see script [snowflake_to_exasol.sql](snowflake_to_exasol.sql)


### SQL Server

The [sqlserver_to_exasol.sql](sqlserver_to_exasol.sql) script generates the statements to migrate a Microsoft
SQL Server **or Azure SQL** database (SQL Server 2016–2025, including the new `json` and `vector` types) to
Exasol v8. It runs on the **target** Exasol database, reads the **source** metadata through a JDBC connection
and **returns** the statements to recreate and load the source. It changes nothing itself — you review the
output and run it, in the order returned. *(This script replaces the former `azure_sql_to_exasol.sql`.)*

**Step by step**
* **Install** the script on the **target** database (run [sqlserver_to_exasol.sql](sqlserver_to_exasol.sql)
  once; it creates `DATABASE_MIGRATION.SQLSERVER_TO_EXASOL`).
* **Install the JDBC driver** in BucketFS: always use the latest Microsoft **`mssql-jdbc`** driver
  ([Maven](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc)). **Do not use the obsolete
  jTDS driver** — it is unstable with current SQL Server versions and with Azure. For Azure
  `authentication=ActiveDirectoryPassword`, also install
  [`azure-identity`](https://mvnrepository.com/artifact/com.azure/azure-identity) (with dependencies). See
  [Load data from SQL Server](https://docs.exasol.com/db/latest/loading_data/connect_sources/sql_server.htm).
* **Create a connection** on the target pointing at the source database. Ready-to-edit `CREATE CONNECTION`
  examples and a connection test (on-prem, Azure, and Azure Entra ID / `ActiveDirectoryPassword`) are at the
  bottom of the script.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it (a few seconds, depending on the
  number of tables).
* **Copy the result set** into another session and execute the statements **in the output order** (the
  CONSTRAINT STATE section runs after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.SQLSERVER_TO_EXASOL(
    'SQLSERVER_JDBC',   -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    false,              -- DB2SCHEMA: false (recommended) => "schema"."table"; true => "database"."schema_table" (migrate several databases at once)
    'mydemo',           -- DB_FILTER: SQL Server database(s): 'mydemo', 'ma%', 'db1, db2', '%' (all)
    '%',                -- SCHEMA_FILTER: schema(s): 'dbo', 'my%', 'schema1, schema2', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema (or database) name
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my%', 't1, t2', '%' (all)
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended for SQL Server) => fold ALL identifiers to UPPER so Exasol queries never need quotes (SQL Server identifiers are case-insensitive, so nothing is lost); false => keep verbatim/quoted (preserves lower/MixedCase, but every query must quote them)
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' (each key ends in its SQL Server state) or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate MS_Description as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY (from the SQL Server partitioning column) inside the CREATE TABLE; false => skip
    'HASHTYPE',         -- BINARY_HANDLING: 'HASHTYPE' (recommended; fixed binary -> HASHTYPE, variable -> hex), 'HEX' (always hex VARCHAR) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; DECIMAL(36,s), import fails for values needing > 36 digits) or 'DOUBLE' (loads with ~15 significant digits)
    false               -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
);
```

This script generates, in this order:
* a prominent **`-- !!! UNSUPPORTED TYPE`** warning for any column the target cannot represent (see below)
* `CREATE SCHEMA` and `CREATE TABLE` — every data type mapped to a sensible Exasol type, plus `NOT NULL`,
  `IDENTITY`, column `DEFAULT`s, the `PRIMARY KEY` (created disabled), and — with `GENERATE_PARTITION_BY` — a
  best-effort `PARTITION BY`
* `ALTER TABLE … ADD … FOREIGN KEY` (created disabled; composite keys supported)
* table & column `COMMENT`s (from `MS_Description`, with `GENERATE_COMMENTS`)
* `IMPORT` of the data (typed transfer — differing source/target NLS does not affect the data; `datetime2`
  fractional precision and `datetimeoffset` as a UTC instant are preserved)
* a **CONSTRAINT STATE** section to run after the IMPORTs (keys are created disabled for a fast,
  order-independent load; this section then sets them per `CONSTRAINT_STATE`)
* with `GENERATE_VIEWS`: the source views as a **commented** manual-review section (T-SQL is not
  auto-translated)

**Data types & limitations.** Mapping is by base system type, so **alias user-defined types resolve to their
base type automatically**; **CLR/assembly UDTs and unknown types are skipped with a prominent warning**.
Character columns are mapped to **`UTF8`** (lossless for any code page). Most types map exactly
(`datetime2(n)` keeps full precision); a few map with a small, documented difference — `float/real → DOUBLE`,
`smalldatetime → TIMESTAMP(0)`, `datetimeoffset → TIMESTAMP(n) WITH LOCAL TIME ZONE` (UTC instant),
`time → VARCHAR`, `rowversion/binary/varbinary/image → HASHTYPE/hex`, `xml/json/vector/sql_variant → VARCHAR`,
`geometry/geography → GEOMETRY` (WKT, SRID not kept), `char/nchar > 2000 → VARCHAR`. The IMPORT **fails
loudly rather than corrupting data** when a value needs more than 36 decimal digits (`DECIMAL_OVERFLOW='CAP'`)
or exceeds 2,000,000 characters (unless `TRUNCATE_LONG_STRINGS=true`). **Always excluded** (so only real user
data/structures appear): the built-in **system schemas** (`sys`, `INFORMATION_SCHEMA`, `guest`, the fixed
`db_*` role schemas), **Microsoft-shipped objects** (`is_ms_shipped`, e.g. `sysdiagrams`, `dtproperties`,
`spt_*`, replication/CDC) and **external/"virtual" tables** (`is_external`); the user's own schemas (incl.
`dbo`) are kept. Not migrated (out of scope): indexes, `UNIQUE`/`CHECK` constraints,
functions/procedures/triggers, users/roles/permissions. See the script header for the full mapping table.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees
— and generates statements for — only the objects that user may access. **To migrate everything, use a user
with sufficient privileges on the source (e.g. `db_owner` / `VIEW DEFINITION`).**

See the header of [sqlserver_to_exasol.sql](sqlserver_to_exasol.sql) for more information!


### Teradata

The [teradata_to_exasol.sql](teradata_to_exasol.sql) script generates the statements to migrate a Teradata
database (**Teradata Vantage 20**, backward compatible with earlier Teradata versions) to Exasol v8. It runs on
the **target** Exasol database, reads the **source** metadata through a JDBC connection and **returns** the
statements to recreate and load the source. It changes nothing itself — you review the output and run it, in the
order returned.

**Step by step**
* **Install** the script on the **target** database (run [teradata_to_exasol.sql](teradata_to_exasol.sql) once;
  it creates `DATABASE_MIGRATION.TERADATA_TO_EXASOL`).
* **Install the JDBC driver** in BucketFS: use the Teradata **`terajdbc`** driver, version **20.00.00.58 or
  higher** ([Maven](https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc)). See
  [Load data from Teradata](https://docs.exasol.com/db/latest/loading_data/connect_sources/teradata.htm) and the
  [Teradata → Exasol migration guide](https://docs.exasol.com/db/latest/migration_guides/teradata/teradata_exasol.htm).
* **Create a connection** on the target pointing at the source database. A ready-to-edit `CREATE CONNECTION`
  example and a connection test are at the bottom of the script. Use the JDBC URL parameter **`DBS_PORT=1025`**
  (the Teradata default) and **`CHARSET=UTF16`** (recommended for Unicode data; `CHARSET=UTF8` also works with
  this script because character columns are sized correctly); a default `DATABASE=` can also be set there.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it (a few seconds, depending on the number
  of tables).
* **Copy the result set** into another session and execute the statements **in the output order** (the
  CONSTRAINT STATE section, and — if enabled — the DATA VALIDATION section, run after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.TERADATA_TO_EXASOL(
    'TERADATA_JDBC',    -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes (Teradata identifiers are case-insensitive, so nothing is lost); false => keep verbatim/quoted (preserves lower/MixedCase, but every query must quote them)
    '%',                -- SCHEMA_FILTER: source database(s)/schema(s): 'CORE', 'MART_%', '%' (all; system databases are always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'H_EMPLOYEE', 'H_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source database name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Teradata comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_DISTRIBUTION_BY: true => map the Teradata Primary Index to an Exasol DISTRIBUTE BY; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from the Teradata partitioning column (single-column RANGE_N); complex PPI (CASE_N / multi-level / expression) is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; BYTE/VARBYTE/BLOB migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; DECIMAL(36,s), import fails for values needing > 36 digits) or 'DOUBLE' (loads with ~15 significant digits)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'INTERVAL',         -- INTERVAL_HANDLING: 'INTERVAL' (recommended; native Exasol INTERVAL, computable) or 'VARCHAR' (interval as text)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build per-table "<table>_MIG_CHK" metric tables and a "<schema>_MIG_CHK" summary that compares source vs. target (run after the IMPORTs)
);
```

This script generates, in this order:
* a prominent **`-- !!! UNSUPPORTED TYPE`** warning for any column the target cannot represent
* `CREATE SCHEMA` and `CREATE TABLE` — every data type mapped to a sensible Exasol type, plus `NOT NULL`,
  column `DEFAULT`s and the `PRIMARY KEY` (created disabled)
* `ALTER TABLE … ADD … FOREIGN KEY` (created disabled; composite keys supported; Teradata's unnamed foreign
  keys get a deterministic generated name; keys to tables outside the migration scope are skipped)
* with `GENERATE_DISTRIBUTION_BY`: `ALTER TABLE … DISTRIBUTE BY` from the Teradata Primary Index
* with `GENERATE_PARTITION_BY`: `ALTER TABLE … PARTITION BY` from the Teradata partitioning column (best-effort;
  complex PPI as a commented review note — see below)
* table & column `COMMENT`s (with `GENERATE_COMMENTS`)
* `IMPORT` of the data (typed transfer — differing source/target NLS does not affect the data)
* a **CONSTRAINT STATE** section to run after the IMPORTs (keys are created disabled for a fast,
  order-independent load; this section then sets them per `CONSTRAINT_STATE`)
* with `GENERATE_VIEWS`: the source views as a **commented** manual-review section (Teradata SQL is not
  auto-translated)
* with `CHECK_MIGRATION`: a **DATA VALIDATION** section (see below)

**Data types & limitations.** Most types map exactly. Integers map to `DECIMAL(p,0)`, `NUMBER/DECIMAL(p,s)` to
`DECIMAL(p,s)`, `FLOAT` to `DOUBLE`. Character columns are mapped to **`UTF8`** (lossless for Unicode/multibyte
data); `CHAR > 2000` becomes `VARCHAR`. `DATE` maps exactly; `TIMESTAMP(n)` keeps full fractional precision;
**`TIMESTAMP(n) WITH TIME ZONE → TIMESTAMP(n) WITH LOCAL TIME ZONE`** (stored as the correct UTC instant);
`TIME`/`TIME WITH TIME ZONE → VARCHAR` (lossless text, offset kept). **`INTERVAL`** maps to a native Exasol
`INTERVAL` (or `VARCHAR`, see `INTERVAL_HANDLING`). **`PERIOD(x)` becomes two columns** `x_BEGINNING` / `x_END`.
**`ST_GEOMETRY`/`MBR`/`MBB` → `GEOMETRY`** (WKT); `CLOB`/`JSON`/`XML`/`DATASET` and other UDTs → `VARCHAR`.
**Binary** (`BYTE`/`VARBYTE`/`BLOB`) is migrated **losslessly as base64 text** (`BINARY_HANDLING='BASE64'`; Exasol
has no general binary column type — the bytes are preserved and can be decoded downstream); values larger than
~48000 bytes exceed the Teradata transfer limit and are loaded as `NULL`. The IMPORT **fails loudly rather than
corrupting data** when a value needs more than 36 decimal digits (`DECIMAL_OVERFLOW='CAP'`) or exceeds 2,000,000
characters (unless `TRUNCATE_LONG_STRINGS=true`). **Always excluded** (so only real user data appears): all
Teradata **system databases** (`DBC`, `Sys*`, `TD_*`, `TDaaS_*`, `SYSLIB`, `SYSSPATIAL`, `val`, … — current as
of Vantage 20). Not migrated (out of scope): secondary/join/hash indexes, `UNIQUE`/`CHECK` constraints
(unsupported by Exasol), macros/procedures/functions, users/roles/rights. See the script header for the full
mapping table.

**Distribution & partitioning.** The Teradata **Primary Index** is mapped to an Exasol `DISTRIBUTE BY`
(`GENERATE_DISTRIBUTION_BY`). For partitioning (`GENERATE_PARTITION_BY`), a **single-column `RANGE_N`** partition
is mapped **best-effort** to an Exasol `PARTITION BY` on that column (Exasol partitions by column value, a
recommended pattern for e.g. a date column). Teradata partitioning that has no single-column Exasol equivalent —
`CASE_N`, multi-level, or an expression instead of a plain column — is emitted as a **commented manual-review
note** rather than applied, so nothing is silently mismapped.

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table holding standardized, cross-database-comparable metrics (row count, per-column NULL counts, distinct
counts, numeric MIN/MAX/SUM, character length MIN/MAX) computed on **both** Teradata and Exasol, plus a
`DATABASE_MIGRATION."<schema>_MIG_CHK"` summary that lists every metric side by side with an **`OK` / `DEVIATION`**
status. Review deviations with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees —
and generates statements for — only the objects that user may access. **To migrate everything, use a user with
sufficient privileges on the source (e.g. `DBC` or a user with the equivalent rights on `DBC.*V` views).**

See the header of [teradata_to_exasol.sql](teradata_to_exasol.sql) for more information!


### Vectorwise

See script [vectorwise_to_exasol.sql](vectorwise_to_exasol.sql)

### Vertica

See script [vertica_to_exasol.sql](vertica_to_exasol.sql)

## Post-load optimization

This folder contains scripts that can be used after having imported data from another database via the scripts above.
What they do:
- Optimize the column's datatypes to minimize storage space on disk
- Import primary keys from other databases

## Delta import

This folder contains a script that can be used if you want to import data on a regular basis.
What it does:
- Import only data that hasn't been imported yet by performing a delta import based on a given column (further explaination [inside the folder](delta_import))
