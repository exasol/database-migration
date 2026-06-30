# Database migration


> ## ⚠️ Please note
>
> This is an **open source project** and is **not officially supported by Exasol**. We are happy to help
> wherever we can, but — since this is not an official Exasol product — **we cannot give any guarantees**.

## Table of Contents
1. [Overview](#overview)
2. [Migration source:](#migration-source)
    * [Azure Blob Storage](#azure-blob-storage)
    * [CSV](#csv)
    * [Db2](#db2)
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


### Db2

The [db2_to_exasol.sql](db2_to_exasol.sql) script generates the statements to migrate an **IBM Db2 for
Linux/Unix/Windows** database (Db2 11.x / 12.x) to Exasol v8. It runs on the **target** Exasol database, reads the
**source** metadata through a JDBC connection (the native `SYSCAT` catalog) and **returns** the statements to
recreate and load the source. It changes nothing itself — you review the output and run it, in the order returned.

**Step by step**
* **Install** the script on the **target** database (run [db2_to_exasol.sql](db2_to_exasol.sql) once; it creates
  `DATABASE_MIGRATION.DB2_TO_EXASOL`).
* **Install the JDBC driver** in BucketFS: use the IBM Data Server Driver for JDBC and SQLJ (**`jcc`**)
  ([Maven](https://mvnrepository.com/artifact/com.ibm.db2/jcc)). See
  [Load data from Db2](https://docs.exasol.com/db/latest/loading_data/connect_sources/db2.htm).
* **Create a connection** on the target pointing at the source database. A ready-to-edit `CREATE CONNECTION`
  example and a connection test are at the bottom of the script.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it.
* **Copy the result set** into another session and execute the statements **in the output order** (the CONSTRAINT
  STATE section, and — if enabled — the DATA VALIDATION section, run after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.DB2_TO_EXASOL(
    'DB2_JDBC',         -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'DB2INST1', 'APP_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'MY_TABLE', 'MY_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Db2 comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => best-effort PARTITION BY from a single-column Db2 range key; complex partitioning is listed as a commented review note; false => skip
    true,               -- GENERATE_DISTRIBUTION_BY: true (default) => add DISTRIBUTE BY from the Db2 DISTRIBUTE BY HASH key; false => skip
    'HEX',              -- BINARY_HANDLING: 'HEX' (recommended; binary/blob migrated losslessly as hex text - Db2 has no base64) or 'SKIP' (load NULL)
    'VARCHAR',          -- DECFLOAT_HANDLING: 'VARCHAR' (recommended; lossless text, keeps all 16/34 digits) or 'DOUBLE' (~15-16 significant digits)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build per-table "<table>_MIG_CHK" metric tables and a "<schema>_MIG_CHK" summary that compares source vs. target (run after the IMPORTs)
);
```

This script generates, in this order: `CREATE SCHEMA` / `CREATE TABLE` (every type mapped, plus `NOT NULL`,
`DEFAULT`s and the `PRIMARY KEY`, created disabled); `FOREIGN KEY`s (disabled, composite supported); a best-effort
`PARTITION BY` / `DISTRIBUTE BY`; table & column `COMMENT`s; the `IMPORT`s; a **CONSTRAINT STATE** section to run
after the load; the source views as a **commented** review section; and (with `CHECK_MIGRATION`) a **DATA
VALIDATION** section.

**Data types & limitations.** Every Db2 type is covered (no silent drops). `SMALLINT`/`INTEGER`/`BIGINT` →
`DECIMAL(5/10/19,0)`; `DECIMAL`/`NUMERIC(p,s)` → `DECIMAL(p,s)`; **`DECFLOAT`** → `VARCHAR` (lossless, keeps all
16/34 digits) or `DOUBLE` (`DECFLOAT_HANDLING`); `REAL`/`DOUBLE` → `DOUBLE`. `DATE` → `DATE`; **`TIME`** →
`VARCHAR(8)` (`HH:MM:SS`); **`TIMESTAMP(p)`** → `TIMESTAMP(min(p,9))` (Exasol's maximum precision is 9). Character:
`CHAR`/`VARCHAR` → `CHAR`/`VARCHAR` `UTF8` (char > 2000 → VARCHAR); `CLOB`/`LONG VARCHAR` → `VARCHAR(2000000)`;
**`GRAPHIC`/`VARGRAPHIC`/`DBCLOB`** (double-byte) → `CHAR`/`VARCHAR` `UTF8`. **Binary** (`CHAR`/`VARCHAR FOR BIT
DATA`, `BINARY`, `VARBINARY`, `BLOB`, `ROWID`) → **hex text** (`BINARY_HANDLING`; Db2 has no base64). `XML` →
`VARCHAR` (via `XMLSERIALIZE`); `BOOLEAN` → `BOOLEAN`. **DISTINCT-type UDTs** are resolved via `SYSCAT.DATATYPES`
to their source built-in and migrated as that base type. The IMPORT **fails loudly rather than corrupting data**
when a value exceeds 2,000,000 characters (unless `TRUNCATE_LONG_STRINGS=true`). **Db2 binary > 16,336 bytes**
hits Db2's `HEX` limit (FOR BIT DATA fails loudly; BLOB is truncated — use `BINARY_HANDLING='SKIP'` to avoid
partial data). **Always excluded** (so only real user data appears): the Db2 system schemas (`SYS*`, `NULLID`).
Not migrated (out of scope): indexes, `UNIQUE`/`CHECK` constraints, triggers, routines, sequences, MQTs.
`IDENTITY` and `GENERATED` columns are migrated as plain columns carrying their values.

**Why some columns are read with a function/cast on the source.** Verified live with jcc 12.1.5.0: the driver
cannot transfer `DECFLOAT`, `GRAPHIC`/`VARGRAPHIC`/`DBCLOB`, `BLOB` or DISTINCT-UDT values directly ("unknown JDBC
type"), so they are read via `CAST(.. AS VARCHAR/base)` and `HEX(..)`; `TIME` is read via `REPLACE(CHAR(..),
'.',':')` → `HH:MM:SS`; `XML` via `XMLSERIALIZE`; column aliases are ignored by the driver, so every metadata
IMPORT carries an explicit derived column list.

**Partitioning & distribution.** A single-column Db2 **range-partition** key is mapped to an Exasol `PARTITION
BY` (`GENERATE_PARTITION_BY`), and the Db2 **`DISTRIBUTE BY HASH`** key to an Exasol `DISTRIBUTE BY`
(`GENERATE_DISTRIBUTION_BY`, default true) — both verified live. Complex / multi-column / expression
partitioning is emitted as a commented manual-review note.

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table of standardized, cross-database-comparable metrics (row count, per-column NULL counts, numeric MIN/MAX/SUM,
date/timestamp MIN/MAX) computed on **both** Db2 and Exasol, plus a `DATABASE_MIGRATION."<schema>_MIG_CHK"`
summary flagging each metric **`OK` / `DEVIATION`**. Review with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees only
the objects that user may access. **To migrate everything, use a user with sufficient privileges on the source.**

See the header of [db2_to_exasol.sql](db2_to_exasol.sql) for more information!


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
  ,'FORCE_DISABLE' -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' (each key ends in its source ENABLED/DISABLED state) or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
  ,'8'           -- TARGET_VERSION: '8' (default) or '7' (downgrade: TIMESTAMP(p) -> TIMESTAMP)
);
```

This script generates, in this order:
* `CREATE SCHEMA` and `CREATE TABLE` — columns keep their **exact source type** (so every data type *and* its
  character set `ASCII`/`UTF8` is reproduced 1:1), plus `NOT NULL`, `IDENTITY` and column `DEFAULT`s; primary
  keys are created **disabled** (in constraint order)
* `ALTER TABLE … ADD … FOREIGN KEY` (created **disabled**)
* `ALTER TABLE … PARTITION BY` and `ALTER TABLE … DISTRIBUTE BY`
* table & column `COMMENT`s
* `IMPORT` of the data (typed transfer — differing source/target NLS does not affect the data; nanosecond
  `TIMESTAMP(9)` is preserved over both EXA and JDBC)
* a **CONSTRAINT STATE** section to run after the import (primary/foreign keys are always created disabled so
  loading is much faster and order-independent; this section sets each key's final state per `CONSTRAINT_STATE`
  — `FORCE_DISABLE` keeps them disabled, `SET_AS_SOURCE` restores the source state, `FORCE_ENABLE` enables every
  key — primary keys before foreign keys)
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

The [mariadb_to_exasol.sql](mariadb_to_exasol.sql) script generates the statements to migrate a MariaDB database
(**MariaDB 10.5+ / 11.x / 12.x**) to Exasol v8. It runs on the **target** Exasol database, reads the **source**
metadata through a JDBC connection and **returns** the statements to recreate and load the source. It changes
nothing itself — you review the output and run it, in the order returned. *(MariaDB is a fork of MySQL; this
script shares most of the `mysql_to_exasol.sql` mapping but handles the MariaDB-specific behavior below.)*

**Step by step**
* **Install** the script on the **target** database (run [mariadb_to_exasol.sql](mariadb_to_exasol.sql) once; it
  creates `DATABASE_MIGRATION.MARIADB_TO_EXASOL`).
* **Install the JDBC driver** in BucketFS: use the latest MariaDB **`mariadb-java-client`** driver
  ([Maven](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client)). See
  [Load data from MariaDB](https://docs.exasol.com/db/latest/loading_data/connect_sources/mariadb.htm).
* **Create a connection** on the target pointing at the source database. A ready-to-edit `CREATE CONNECTION`
  example and a connection test are at the bottom of the script.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it.
* **Copy the result set** into another session and execute the statements **in the output order** (the
  CONSTRAINT STATE section, and — if enabled — the DATA VALIDATION section, run after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.MARIADB_TO_EXASOL(
    'MARIADB_JDBC',     -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source database(s): 'mydb', 'sales_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate MariaDB comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from a single-column MariaDB partition key; complex partitioning is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; binary/blob migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; decimal>36 -> DECIMAL(36,s); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 significant digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'NULL',             -- TEMPORAL_OUT_OF_RANGE: 'NULL' (recommended for MariaDB; zero-date -> NULL, matching the driver), 'CLAMP' (-> 0001-01-01) or 'FAIL' (IMPORT fails loudly on a zero-date)
    false,              -- TINYINT1_AS_BOOLEAN: false (recommended; tinyint(1) -> DECIMAL(3,0), value preserved) or true (tinyint(1) -> BOOLEAN)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build per-table "<table>_MIG_CHK" metric tables and a "<schema>_MIG_CHK" summary that compares source vs. target (run after the IMPORTs)
);
```

This script generates, in this order: `CREATE SCHEMA` / `CREATE TABLE` (every type mapped, plus `NOT NULL`,
`DEFAULT`s and the `PRIMARY KEY`, created disabled); `FOREIGN KEY`s (disabled, composite supported); a
best-effort `PARTITION BY`; table & column `COMMENT`s; the `IMPORT`s; a **CONSTRAINT STATE** section to run after
the load; the source views as a **commented** review section; and (with `CHECK_MIGRATION`) a **DATA VALIDATION**
section.

**Data types & limitations.** Every MariaDB type is covered (no silent drops). Integers map to
`DECIMAL(3/5/7/10/19,0)` — **`UNSIGNED` widens** `mediumint`→`DECIMAL(8,0)` and `bigint`→`DECIMAL(20,0)`;
`decimal(p,s)`→`DECIMAL(p,s)`, `float`/`double`→`DOUBLE`, `bit(M)`→`DECIMAL`. `date`→`DATE`; `datetime(p)` keeps
full precision; **`timestamp(p)`→`TIMESTAMP(p) WITH LOCAL TIME ZONE`**, `datetime(p)`→`TIMESTAMP(p)` (wall clock).
Character columns map to **`UTF8`**; `char>2000`→`VARCHAR`; `tinytext…longtext`/`json`→`VARCHAR(2000000)`
(**MariaDB `JSON` is an alias for `LONGTEXT`**); **`enum`/`set`→`VARCHAR`**. **MariaDB-only native types**:
**`UUID`→`CHAR(36)`**, **`INET4`→`VARCHAR(15)`**, **`INET6`→`VARCHAR(45)`**. **`binary`/`varbinary`/`*blob`→
base64 text** (`BINARY_HANDLING`). `time`→`VARCHAR(17)` (Exasol has no TIME type; MariaDB `TIME` spans
`-838:59:59 … 838:59:59`), `year`→`VARCHAR(4)`, spatial types→**`GEOMETRY`** (WKT). **`tinyint(1)`**→`DECIMAL(3,0)`
(value preserved; the driver otherwise coerces it to boolean), or `BOOLEAN` with `TINYINT1_AS_BOOLEAN=true`.
**`decimal` with > 36 digits** is handled via `DECIMAL_OVERFLOW`. The IMPORT **fails loudly rather than
corrupting data** when a value needs more than 36 decimal digits (`CAP`) or exceeds 2,000,000 characters (unless
`TRUNCATE_LONG_STRINGS=true`). **Always excluded** (so only real user data appears): the MariaDB **system
schemas** (`mysql`, `information_schema`, `performance_schema`, `sys`) and **sequences**. Not migrated (out of
scope): indexes, `UNIQUE`/`CHECK` constraints, triggers, routines, events. `AUTO_INCREMENT` and `STORED`/`VIRTUAL`
generated columns are migrated as plain columns carrying their values.

**Defaults & casts (MariaDB specifics).** MariaDB's `information_schema` returns SQL-literal-formatted defaults
(a no-default column reads as `'NULL'`, string defaults are already quoted, `CURRENT_TIMESTAMP` reads as
`current_timestamp()`); they are passed through faithfully. Some columns must be read with a `CAST` on the source
(verified with Connector/J 3.5.9): `UNSIGNED` integers and `BIT` exceed their signed Java type (and overflow on a
direct read), `YEAR` is returned as a `DATE`, `TIME` keeps its range/fraction, and the native `UUID` type is not
transferable directly — all are read via `CAST(.. AS CHAR)` into the target.

**Zero-dates.** MariaDB allows `0000-00-00` and the MariaDB driver converts it to `NULL` on read, so the default
`TEMPORAL_OUT_OF_RANGE='NULL'` loads such values as `NULL`; `CLAMP` maps them to `0001-01-01` and `FAIL` makes the
IMPORT fail loudly.

**Sequences & system-versioned tables.** `CREATE SEQUENCE` objects are **skipped** (Exasol has no sequence type).
**System-versioned** tables (`WITH SYSTEM VERSIONING`) are migrated as a normal table holding their **current**
rows (Exasol has no system-versioning; the hidden period columns are not migrated and are dropped from the PK).

**Partitioning.** A single-column MariaDB partition key is mapped best-effort to an Exasol `PARTITION BY`;
`HASH`/`KEY`/expression partitioning is emitted as a commented review note. A MariaDB partitioned table is one
logical table, so data is never migrated twice. (MariaDB has no distribution concept, so no `DISTRIBUTE BY`.)

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table of standardized, cross-database-comparable metrics computed on **both** MariaDB and Exasol, plus a
`DATABASE_MIGRATION."<schema>_MIG_CHK"` summary flagging each metric **`OK` / `DEVIATION`**. Review with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees only
the objects that user may access. **To migrate everything, use a user with sufficient privileges on the source.**

See the header of [mariadb_to_exasol.sql](mariadb_to_exasol.sql) for more information!


### MySQL

The [mysql_to_exasol.sql](mysql_to_exasol.sql) script generates the statements to migrate a MySQL
database (**MySQL 8 / 9**, backward compatible with earlier 5.x) to Exasol v8. It runs on the **target** Exasol
database, reads the **source** metadata through a JDBC connection and **returns** the statements to recreate and
load the source. It changes nothing itself — you review the output and run it, in the order returned.

**Step by step**
* **Install** the script on the **target** database (run [mysql_to_exasol.sql](mysql_to_exasol.sql) once; it
  creates `DATABASE_MIGRATION.MYSQL_TO_EXASOL`).
* **Install the JDBC driver** in BucketFS: use the latest MySQL **`mysql-connector-j`** driver
  ([Maven](https://mvnrepository.com/artifact/com.mysql/mysql-connector-j)). See
  [Load data from MySQL](https://docs.exasol.com/db/latest/loading_data/connect_sources/mysql.htm).
* **Create a connection** on the target pointing at the source database. A ready-to-edit `CREATE CONNECTION`
  example and a connection test are at the bottom of the script.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it (a few seconds, depending on the number
  of tables).
* **Copy the result set** into another session and execute the statements **in the output order** (the
  CONSTRAINT STATE section, and — if enabled — the DATA VALIDATION section, run after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.MYSQL_TO_EXASOL(
    'MYSQL_JDBC',       -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source database(s): 'mydb', 'sales_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate MySQL comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from a single-column MySQL partition key; complex partitioning is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; binary/blob migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; decimal>36 -> DECIMAL(36,s); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 significant digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'FAIL',             -- TEMPORAL_OUT_OF_RANGE: 'FAIL' (recommended; IMPORT fails on a zero-date / out-of-range date), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min)
    false,              -- TINYINT1_AS_BOOLEAN: false (recommended; tinyint(1) -> DECIMAL(3,0), value preserved) or true (tinyint(1) -> BOOLEAN)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build per-table "<table>_MIG_CHK" metric tables and a "<schema>_MIG_CHK" summary that compares source vs. target (run after the IMPORTs)
);
```

This script generates, in this order:
* `CREATE SCHEMA` and `CREATE TABLE` — every data type mapped to a sensible Exasol type, plus `NOT NULL`,
  column `DEFAULT`s and the `PRIMARY KEY` (created disabled)
* `ALTER TABLE … ADD … FOREIGN KEY` (created disabled; composite keys supported; keys to tables outside the
  migration scope are skipped)
* with `GENERATE_PARTITION_BY`: `ALTER TABLE … PARTITION BY` from a single-column MySQL partition key (best-effort)
* table & column `COMMENT`s (with `GENERATE_COMMENTS`)
* `IMPORT` of the data (typed transfer — differing source/target NLS does not affect the data)
* a **CONSTRAINT STATE** section to run after the IMPORTs (keys created disabled for a fast, order-independent
  load; this section then sets them per `CONSTRAINT_STATE`)
* with `GENERATE_VIEWS`: the source views as a **commented** manual-review section (MySQL SQL is not
  auto-translated)
* with `CHECK_MIGRATION`: a **DATA VALIDATION** section (see below)

**Data types & limitations.** Every MySQL type is covered (no silent drops). Integers map to
`DECIMAL(3/5/7/10/19,0)` — **`UNSIGNED` widens** `mediumint`→`DECIMAL(8,0)` and `bigint`→`DECIMAL(20,0)`;
`decimal(p,s)`→`DECIMAL(p,s)`, `float`/`double`→`DOUBLE`, `bit(M)`→`DECIMAL`. `date`→`DATE`; `datetime(p)` keeps
full precision; **`timestamp(p)`→`TIMESTAMP(p) WITH LOCAL TIME ZONE`** (the tz-aware instant type), `datetime(p)`→
`TIMESTAMP(p)` (wall clock). Character columns map to **`UTF8`**; `char>2000`→`VARCHAR`; `tinytext…longtext`/
`json`→`VARCHAR(2000000)`; **`enum`/`set`→`VARCHAR`** (label / CSV). **`binary`/`varbinary`/`*blob`→base64 text**
(`BINARY_HANDLING`, lossless, decode downstream). `time`→`VARCHAR(17)` (Exasol has no TIME type; MySQL `TIME`
spans `-838:59:59 … 838:59:59` and keeps fractional seconds); `year`→`VARCHAR(4)`; spatial types→**`GEOMETRY`**
(WKT via `ST_AsText`). **`tinyint(1)`** → `DECIMAL(3,0)` (value preserved; the JDBC driver otherwise coerces it
to boolean, collapsing any non‑0/1 to 1), or `BOOLEAN` with `TINYINT1_AS_BOOLEAN=true`. **`decimal` with > 36
digits** is handled via `DECIMAL_OVERFLOW` (`CAP` / `DOUBLE` / `VARCHAR`). The IMPORT **fails loudly rather than
corrupting data** when a value needs more than 36 decimal digits (`CAP`), exceeds 2,000,000 characters (unless
`TRUNCATE_LONG_STRINGS=true`), or is a zero-date / out-of-range date (`TEMPORAL_OUT_OF_RANGE='FAIL'`; `NULL` or
`CLAMP` are available — see the optional `zeroDateTimeBehavior` driver note at the bottom of the script).
**Always excluded** (so only real user data appears): the MySQL **system schemas** (`mysql`,
`information_schema`, `performance_schema`, `sys`). Not migrated (out of scope): indexes, `UNIQUE`/`CHECK`
constraints, triggers, routines, events, users/grants. `AUTO_INCREMENT` columns and `STORED`/`VIRTUAL` generated
columns are migrated as plain columns carrying their values (Exasol has no auto-increment / computed columns).

**Why some columns are read with a `CAST` on the source.** Verified live with Connector/J 9.7: `UNSIGNED`
integers exceed their signed Java type (e.g. `SMALLINT UNSIGNED` 60000 overflows `java.lang.Short`;
`BIGINT UNSIGNED` / `BIT(64)` overflow `java.lang.Long`), so every unsigned integer / bit is transferred as text
into a `DECIMAL` target; `YEAR` is returned as a `DATE` by the driver, so it is read with `CAST(.. AS CHAR)`;
`TIME` likewise, to keep its full range and fractional seconds.

**Partitioning.** A single-column MySQL partition key (e.g. `RANGE COLUMNS(sale_date)`) is mapped best-effort to
an Exasol `PARTITION BY` on that column; `HASH`/`KEY`/expression partitioning is emitted as a commented
manual-review note. A MySQL partitioned table is one logical table, so data is never migrated twice. (MySQL has
no distribution/clustering-key concept, so no `DISTRIBUTE BY` is generated.)

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table holding standardized, cross-database-comparable metrics (row count, per-column NULL counts, distinct
counts, numeric MIN/MAX/SUM, character length MIN/MAX) computed on **both** MySQL and Exasol, plus a
`DATABASE_MIGRATION."<schema>_MIG_CHK"` summary that lists every metric side by side with an **`OK` / `DEVIATION`**
status. Review deviations with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees —
and generates statements for — only the objects that user may access. **To migrate everything, use a user with
sufficient privileges on the source.**

See the header of [mysql_to_exasol.sql](mysql_to_exasol.sql) for more information!


### Netezza

The [netezza_to_exasol.sql](netezza_to_exasol.sql) script generates the statements to migrate an **IBM Netezza
Performance Server (NPS)** database (7.x / 11.x) to Exasol v8. It runs on the **target** Exasol database, reads the
**source** metadata through a JDBC connection (the native `_V_*` catalog) and **returns** the statements to
recreate and load the source. It changes nothing itself — you review the output and run it, in the order returned.

**Step by step**
* **Install** the script on the **target** database (run [netezza_to_exasol.sql](netezza_to_exasol.sql) once; it
  creates `DATABASE_MIGRATION.NETEZZA_TO_EXASOL`).
* **Install the Netezza JDBC driver in BucketFS** — **this is required before the connection can be created.** The
  driver is **not on Maven and not publicly downloadable**, so it must be obtained from IBM and uploaded together
  with a `settings.cfg`:
    1. **Download `nzjdbc3.jar` from IBM Fix Central** (a free IBM registration is required): search for
       **"IBM Cloud Pak for Data System"**, select release **`NPS_11.3`**, and download
       ([direct link](https://www.ibm.com/support/fixcentral/swg/selectFixes?parent=ibm%7EWebSphere&product=ibm/WebSphere/IBM+Cloud+Private+for+Data+System&release=NPS_11.3&platform=All&function=all)).
       IBM help: [installing client tools](https://www.ibm.com/docs/en/netezza?topic=dls-installing-uninstalling-client-tools-software-2)
       · [client software packages](https://www.ibm.com/docs/en/netezza?topic=iucts-client-software-packages#c_datacon_client_sw_packages).
    2. **Create a plain-text `settings.cfg`** with exactly this content:
       ```
       DRIVERNAME=NETEZZA
       DRIVERMAIN=org.netezza.Driver
       PREFIX=jdbc:netezza:
       NOSECURITY=YES
       FETCHSIZE=100000
       INSERTSIZE=-1
       ```
    3. **Upload both `nzjdbc3.jar` and `settings.cfg` to BucketFS** (Exasol "add a JDBC driver":
       [on-premise guide](https://docs.exasol.com/db/latest/administration/on-premise/manage_drivers/add_jdbc_driver.htm)
       · [SaaS guide](https://docs.exasol.com/db/latest/administration/manage_drivers/add_jdbc_driver.htm)).
       On-premise example (set `WRITE_PW` and `DATABASE_NODE_IP` to your values):
       ```bash
       curl -k -X PUT -T settings.cfg https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/netezza/settings.cfg
       curl -k -X PUT -T nzjdbc3.jar  https://w:$WRITE_PW@$DATABASE_NODE_IP:2581/default/drivers/jdbc/netezza/nzjdbc3.jar
       ```
* **Create a connection** on the target. **IMPORTANT:** point it at the **source database to migrate** (e.g.
  `jdbc:netezza://host:5480/MYDB`), **not** at the `SYSTEM` database — Netezza cannot hold user tables in `SYSTEM`
  and its `_V_*` catalog views are database-scoped. A ready-to-edit `CREATE CONNECTION` example and a connection
  test are at the bottom of the script.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it.
* **Copy the result set** into another session and execute the statements **in the output order** (the CONSTRAINT
  STATE section, and — if enabled — the DATA VALIDATION section, run after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.NETEZZA_TO_EXASOL(
    'NETEZZA_JDBC',     -- CONNECTION_NAME: JDBC connection (pointing at the SOURCE database, not SYSTEM)
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes; false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'MYSCHEMA', 'APP_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'MY_TABLE', 'MY_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate Netezza comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_DISTRIBUTION_BY: true (default) => add DISTRIBUTE BY from the Netezza hash distribution key; false => skip
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; numeric>36 -> DECIMAL(36,s); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 digits) or 'VARCHAR' (lossless text)
    'VARCHAR',          -- INTERVAL_HANDLING: 'VARCHAR' (recommended; interval as lossless text) or 'INTERVAL' (native Exasol INTERVAL, best-effort - day-time intervals only)
    'HEX',              -- BINARY_HANDLING: 'HEX' (recommended; BINARY/VARBINARY migrated losslessly as hex text via to_hex - max 32000 bytes) or 'SKIP' (load NULL)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build "<table>_MIG_CHK" metric tables + a "<schema>_MIG_CHK" summary (source vs target) for post-load validation
);
```

This script generates, in this order: `CREATE SCHEMA` / `CREATE TABLE` (every type mapped, plus `NOT NULL`,
`DEFAULT`s and the `PRIMARY KEY`, created disabled); `FOREIGN KEY`s (disabled, composite supported); a
`DISTRIBUTE BY` from the Netezza distribution key; table & column `COMMENT`s; the `IMPORT`s; a **CONSTRAINT STATE**
section to run after the load; the source views as a **commented** review section; and (with `CHECK_MIGRATION`) a
**DATA VALIDATION** section.

**Data types & limitations.** *Every* type this NPS supports was CREATE-probed live; all are covered (no silent
drops). `BYTEINT`/`SMALLINT`/`INTEGER`/`BIGINT` → `DECIMAL(3/5/10/19,0)`; `NUMERIC(p,s)` → `DECIMAL(p,s)` (Netezza
max precision 38; `p > 36` → `DECIMAL_OVERFLOW`); `REAL`/`DOUBLE PRECISION`/`FLOAT` → `DOUBLE`. `CHARACTER`/
`CHARACTER VARYING` and the national `NCHAR`/`NVARCHAR` → `CHAR`/`VARCHAR` `UTF8` (char > 2000 → `VARCHAR`). `DATE`
→ `DATE`; **`TIME`** → `VARCHAR(15)`, **`TIME WITH TIME ZONE`** → `VARCHAR(21)` (Exasol has no `TIME` type);
**`TIMESTAMP`** → `TIMESTAMP(6)` (full microsecond precision). **`INTERVAL`** → `VARCHAR` (lossless) or a best-effort
native Exasol `INTERVAL DAY TO SECOND` (`INTERVAL_HANDLING`). `BOOLEAN` → `BOOLEAN`. **`JSON`/`JSONB`/`JSONPATH`** →
`VARCHAR` (text). **`BINARY`/`VARBINARY`** (reported as `BINARY VARYING`) → `VARCHAR` **hex text** via `to_hex`
(`BINARY_HANDLING`). **`ST_GEOMETRY`** → `VARCHAR` (WKT, best-effort, via `ST_ASTEXT`). A `VARCHAR(2000000)`
catch-all covers anything unexpected (no silent drops). The IMPORT **fails loudly rather than corrupting data** when
a `NUMERIC` needs more than 36 digits under `DECIMAL_OVERFLOW='CAP'`, or a binary value exceeds 32000 bytes under
`BINARY_HANDLING='HEX'` (Netezza's 64000-char VARCHAR limit on the hex text). **Internal data types** (`ROWID`,
`CREATEXID`, `DELETEXID`, `DATASLICEID`) are pseudo-columns not present in the catalog, so they are never migrated.
**Temporal** types are stored internally as integers but read as calendar values (migrated by value, full µs).
**Always excluded** (so only real user data appears): the Netezza system schemas (`DEFINITION_SCHEMA`,
`INFORMATION_SCHEMA`). Not migrated (out of scope): indexes/zone maps, `ORGANIZE ON` (CBT) clustering, `UNIQUE`/
`CHECK` constraints, sequences, procedures, materialized views. **Not present in this NPS** (CREATE rejects them, so
they cannot occur): `MONEY`, `GRAPHIC`/`VARGRAPHIC`, `LONG VARCHAR`, `CLOB`/`BLOB`, `BYTE`/`VARBYTE`, `TIMESTAMP
WITH TIME ZONE`, `XML`, `ARRAY`, `UUID`.

**Why some columns are read with a cast/function on the source.** Verified live with the Netezza JDBC driver
(`nzjdbc3.jar`, NPS 11.3.1.2): the driver cannot transfer some types directly, so the generated IMPORT reads them as
text — `TIME` (`Bad value for NZ_TIME`) and `INTERVAL` (`unknown` JDBC type) and `TIME WITH TIME ZONE` via
`CAST(.. AS VARCHAR)`; **`BINARY`/`VARBINARY`** (raw = "unknown") via **`to_hex(..)`**; **`ST_GEOMETRY`** (raw + cast
both fail) via **`ST_ASTEXT(..)`** (WKT). Everything else — integers, `NUMERIC`, `REAL`/`DOUBLE`, all char types
(incl. multibyte `NCHAR`/`NVARCHAR`), `DATE`, `TIMESTAMP` (full µs), `BOOLEAN`, and **`JSON`/`JSONB`/`JSONPATH`**
(raw transfer works) — is read directly. Column `DEFAULT`s carry a Netezza `::"TYPE"` cast (e.g. `'NEW'::"NVARCHAR"`)
which is stripped. (The driver honours column aliases; explicit derived column lists are emitted anyway for
robustness, as in the other reworks.)

**Distribution.** The Netezza hash distribution key (`DISTRIBUTE ON`) is mapped to an Exasol `DISTRIBUTE BY`
(`GENERATE_DISTRIBUTION_BY`, default true), verified live. Netezza has **no range partitioning** (only hash
distribution + `ORGANIZE ON` clustering), so there is no `GENERATE_PARTITION_BY`; `ORGANIZE ON` is not mapped.

**Intervals.** `INTERVAL_HANDLING='VARCHAR'` (default) migrates the interval as lossless text
(`1 year 2 mons 3 days`). `INTERVAL_HANDLING='INTERVAL'` builds a native Exasol `INTERVAL DAY TO SECOND` from the
day-time components (via Netezza `EXTRACT`) — a best-effort that covers **day-time** intervals; year/month
components and sub-second fractions are not representable in Exasol's `INTERVAL DAY TO SECOND` and are not carried,
so use `VARCHAR` when those occur.

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table of standardized, cross-database-comparable metrics (row count, per-column NULL counts, numeric MIN/MAX/SUM,
date/timestamp MIN/MAX, variable-char min/max length) computed on **both** Netezza and Exasol, plus a
`DATABASE_MIGRATION."<schema>_MIG_CHK"` summary flagging each metric **`OK` / `DEVIATION`**. Review with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees only
the objects that user may access. **To migrate everything, use a user with sufficient privileges on the source.**

See the header of [netezza_to_exasol.sql](netezza_to_exasol.sql) for more information!


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

The [postgresql_to_exasol.sql](postgresql_to_exasol.sql) script generates the statements to migrate a PostgreSQL
database (**PostgreSQL 18**, backward compatible with earlier versions) to Exasol v8. It runs on the **target**
Exasol database, reads the **source** metadata through a JDBC connection and **returns** the statements to
recreate and load the source. It changes nothing itself — you review the output and run it, in the order
returned. *(This script was previously named `postgres_to_exasol.sql`.)*

**Step by step**
* **Install** the script on the **target** database (run [postgresql_to_exasol.sql](postgresql_to_exasol.sql)
  once; it creates `DATABASE_MIGRATION.POSTGRESQL_TO_EXASOL`).
* **Install the JDBC driver** in BucketFS: use the latest PostgreSQL **`postgresql`** driver (42.7.11 or higher)
  ([Maven](https://mvnrepository.com/artifact/org.postgresql/postgresql)). See
  [Load data from PostgreSQL](https://docs.exasol.com/db/latest/loading_data/connect_sources/postgresql.htm).
* **Create a connection** on the target pointing at the source database. A ready-to-edit `CREATE CONNECTION`
  example and a connection test are at the bottom of the script.
* **Adapt the `EXECUTE SCRIPT` parameters** to your scenario and run it (a few seconds, depending on the number
  of tables).
* **Copy the result set** into another session and execute the statements **in the output order** (the
  CONSTRAINT STATE section, and — if enabled — the DATA VALIDATION section, run after the IMPORTs).

```sql
EXECUTE SCRIPT DATABASE_MIGRATION.POSTGRESQL_TO_EXASOL(
    'POSTGRESQL_JDBC',  -- CONNECTION_NAME: name of the JDBC connection created at the bottom of the script
    true,               -- IDENTIFIER_CASE_INSENSITIVE: true (recommended) => fold ALL identifiers to UPPER so Exasol queries never need quotes (PostgreSQL folds unquoted names to lower-case, so nothing is lost); false => keep verbatim/quoted
    '%',                -- SCHEMA_FILTER: source schema(s): 'public', 'sales_%', '%' (all; system schemas always excluded)
    '%',                -- TABLE_FILTER: table(s)/view(s): 'my_table', 'my_%', '%' (all)
    '',                 -- TARGET_SCHEMA: Exasol target schema; '' (recommended) => use the source schema name
    'FORCE_DISABLE',    -- CONSTRAINT_STATE: 'FORCE_DISABLE' (recommended; PK/FK kept as metadata only - faster, order-independent imports, still used by BI tools), 'SET_AS_SOURCE' or 'FORCE_ENABLE' (all keys enabled = Exasol re-validates the data)
    true,               -- GENERATE_COMMENTS: true (recommended) => migrate PostgreSQL comments as COMMENT ON; false => skip
    true,               -- GENERATE_VIEWS: true => emit source views as a commented manual-review section; false => skip
    true,               -- GENERATE_PARTITION_BY: true => add a best-effort PARTITION BY from the PostgreSQL partition key (single column); complex partitioning is listed as a commented manual-review note; false => skip
    'BASE64',           -- BINARY_HANDLING: 'BASE64' (recommended; bytea migrated losslessly as base64 text - Exasol has no general binary type) or 'SKIP' (load NULL)
    'CAP',              -- DECIMAL_OVERFLOW: 'CAP' (recommended; numeric>36 -> DECIMAL(36,s), unconstrained -> DECIMAL(36,18); IMPORT fails for values needing > 36 digits), 'DOUBLE' (~15 significant digits) or 'VARCHAR' (lossless text)
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    'VARCHAR',          -- INTERVAL_HANDLING: 'VARCHAR' (recommended; interval as lossless text) or 'INTERVAL' (native Exasol INTERVAL DAY TO SECOND, best-effort)
    'FAIL',             -- TEMPORAL_OUT_OF_RANGE: 'FAIL' (recommended; IMPORT fails on a date/timestamp outside 0001..9999), 'NULL' (load NULL) or 'CLAMP' (clamp to the Exasol min/max)
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build per-table "<table>_MIG_CHK" metric tables and a "<schema>_MIG_CHK" summary that compares source vs. target (run after the IMPORTs)
);
```

This script generates, in this order:
* a prominent **`-- !!! UNSUPPORTED TYPE`** warning for any column the target cannot represent (only pseudo-types)
* `CREATE SCHEMA` and `CREATE TABLE` — every data type mapped to a sensible Exasol type, plus `NOT NULL`,
  column `DEFAULT`s and the `PRIMARY KEY` (created disabled)
* `ALTER TABLE … ADD … FOREIGN KEY` (created disabled; composite keys supported; keys to tables outside the
  migration scope are skipped)
* with `GENERATE_PARTITION_BY`: `ALTER TABLE … PARTITION BY` from the PostgreSQL partition key (best-effort)
* table & column `COMMENT`s (with `GENERATE_COMMENTS`)
* `IMPORT` of the data (typed transfer — differing source/target NLS does not affect the data)
* a **CONSTRAINT STATE** section to run after the IMPORTs (keys created disabled for a fast, order-independent
  load; this section then sets them per `CONSTRAINT_STATE`)
* with `GENERATE_VIEWS`: the source views as a **commented** manual-review section (PostgreSQL SQL is not
  auto-translated)
* with `CHECK_MIGRATION`: a **DATA VALIDATION** section (see below)

**Data types & limitations.** Mapping is by PostgreSQL type category, so **every type is covered** (no silent
drops) and **domains (including nested domains) resolve to their base type** automatically. Integers map to `DECIMAL(5/10/19,0)`,
`numeric(p,s)` to `DECIMAL(p,s)`, `real`/`double precision` to `DOUBLE`, `money` to `DECIMAL(20,2)`, `boolean`
to `BOOLEAN`. Character columns are mapped to **`UTF8`**; `char > 2000` becomes `VARCHAR`. `date` maps exactly;
`timestamp(p)` keeps full precision; **`timestamp with time zone → TIMESTAMP(p) WITH LOCAL TIME ZONE`** (stored
as the correct UTC instant); `time`/`time with time zone → VARCHAR` (lossless text). `uuid → CHAR(36)`;
**`bytea` → base64 text** (`BINARY_HANDLING`, lossless, decode downstream); `json`/`jsonb`/`xml`, arrays,
ranges/multiranges, enums, geometric, network, bit, `tsvector`, composite → `VARCHAR` (faithful text).
**`interval`** → `VARCHAR` (lossless) or native Exasol `INTERVAL` (`INTERVAL_HANDLING`; best-effort - a
PostgreSQL interval can mix months and days/seconds, which no single Exasol interval type can hold, so native
mode supports pure day-time intervals only). **`numeric` with > 36 digits or no declared precision** is handled
via `DECIMAL_OVERFLOW` (`CAP` / `DOUBLE` / `VARCHAR`). The IMPORT **fails loudly rather than corrupting data**
when a value needs more than 36 decimal digits (`DECIMAL_OVERFLOW='CAP'`), exceeds 2,000,000 characters (unless
`TRUNCATE_LONG_STRINGS=true`), or a date/timestamp falls outside Exasol's `0001-01-01 … 9999-12-31` range
(`TEMPORAL_OUT_OF_RANGE='FAIL'`; `NULL` or `CLAMP` are available). **Always excluded** (so only real user data
appears): the PostgreSQL **system schemas** (`pg_catalog`, `information_schema`, `pg_toast`, `pg_temp*`, any
`pg_*`) and **extension-owned tables** (e.g. PostGIS `spatial_ref_sys`). Not migrated (out of scope): indexes,
`UNIQUE`/`CHECK`/exclusion constraints, sequences, functions/procedures/triggers, users/roles/privileges.

**Partitioning.** PostgreSQL declarative-partition **child** tables are skipped — the partitioned **parent** is
migrated as a single Exasol table holding all rows, so data is never migrated twice. A single-column partition
key is mapped best-effort to an Exasol `PARTITION BY` on that column; multi-column or expression partitioning is
emitted as a commented manual-review note. (PostgreSQL has no distribution/clustering-key concept, so no
`DISTRIBUTE BY` is generated.)

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table holding standardized, cross-database-comparable metrics (row count, per-column NULL counts, distinct
counts, numeric MIN/MAX/SUM, character length MIN/MAX) computed on **both** PostgreSQL and Exasol, plus a
`DATABASE_MIGRATION."<schema>_MIG_CHK"` summary that lists every metric side by side with an **`OK` / `DEVIATION`**
status. Review deviations with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

**Privileges/visibility:** the source metadata is read **through the connection's user**, so the script sees —
and generates statements for — only the objects that user may access. **To migrate everything, use a user with
sufficient privileges on the source.**

See the header of [postgresql_to_exasol.sql](postgresql_to_exasol.sql) for more information!


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
    false,              -- TRUNCATE_LONG_STRINGS: false (recommended) => import fails on a value > 2,000,000 chars; true => cut such values to 2,000,000 chars and import
    false               -- CHECK_MIGRATION: false (recommended default) => skip; true => also build "<table>_MIG_CHK" metric tables + a "<schema>_MIG_CHK" summary (source vs target) for post-load validation
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
* with `CHECK_MIGRATION`: a **DATA VALIDATION** section — per-table `"<table>_MIG_CHK"` metric tables and a
  `"<schema>_MIG_CHK"` summary (run after the IMPORTs)

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

**Migration check (`CHECK_MIGRATION=true`).** For every migrated table the script builds a `"<table>_MIG_CHK"`
table of standardized, cross-database-comparable metrics (row count, per-column NULL counts, numeric MIN/MAX/SUM
on exact integer/decimal types, date/datetime MIN/MAX to the second, DISTINCT counts) computed on **both** SQL
Server and Exasol, plus a `DATABASE_MIGRATION."<schema>_MIG_CHK"` summary flagging each metric **`OK` /
`DEVIATION`**. The metric set is mapping-aware (float/real and binary/LOB/CLR/`json`/`vector` are excluded from
value metrics) so faithful data yields zero deviations. Review with
`SELECT * FROM DATABASE_MIGRATION."<schema>_MIG_CHK" WHERE "STATUS" = 'DEVIATION';`.

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
**`ST_GEOMETRY`/`MBR`/`MBB` → `GEOMETRY`** (WKT); `CLOB`/`JSON`/`XML`/`DATASET` → `VARCHAR`. **Distinct
user-defined types** are resolved to their base predefined type and migrated as that type (numeric → `DECIMAL`/
`DOUBLE`, character → `CHAR`/`VARCHAR`, `DATE` → `DATE`, `TIMESTAMP` → `TIMESTAMP`, byte → base64, …); only
**structured / array UDTs** (no single base type) are unsupported and flagged for manual review.
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
