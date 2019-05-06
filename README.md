# Database migration
[![Build Status](https://travis-ci.org/exasol/database-migration.svg?branch=master)](https://travis-ci.org/exasol/database-migration)

###### Please note that this is an open source project which is *not officially supported* by Exasol. We will try to help you as much as possible, but can't guarantee anything since this is not an official Exasol product.

## Table of Contents
1. [Overview](#overview)
2. [Migration source:](#migration-source)
    * [CSV](#csv)
    * [DB2](#db2)
    * [Exasol](#exasol)
    * [MySQL](#mysql)
    * [Oracle](#oracle)
    * [PostgreSQL](#postgres)
    * [Redshift](#redshift)
    * [S3](#s3)
    * [SAP_Hana](#sap_hana)
    * [SQL Server](#sqlserver)
    * [Teradata](#teradata)
    * [Vectorwise](#vectorwise)
    * [Vertica](#vertica)
    * [Google BigQuery](#google-bigquery)
3. [Post-load optimization](#post-load-optimization)
4. [Delta import](#delta-import)


## Overview

This project contains SQL scripts for automatically importing data from various data management systems into Exasol.

You'll find SQL scripts which you can execute on Exasol to load data from certain databases or
database management systems. The scripts try to extract the meta data from the source system and create the appropriate IMPORT statements automatically so that you don't have to care about table names, column names and types.

If you want to optimize existing scripts or create new scripts for additional systems, we would be very glad if you share your work with the Exasol user community.

## Migration source
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

- Import from **HDFS**: See [SOL-322](https://www.exasol.com/support/browse/SOL-322)

- Import from **S3**: See [Exasol-1774](https://www.exasol.com/support/browse/Exasol-1774) for single file import, for importing multiple files scroll down to [S3](#s3)

For more details on `IMPORT` see paragraph 2.2.2 in the User Manual. For further help on typical CSV-formatting issues, see
* [How to load bad CSV files](https://www.exasol.com/support/browse/SOL-578)
* [Proper csv export from MySQL](https://www.exasol.com/support/browse/SOL-428)
* [Proper csv export from IBM DB2](https://www.exasol.com/support/browse/SOL-430)
* [Proper csv export from Oracle](https://www.exasol.com/support/browse/SOL-440)
* [Proper csv export from PostgreSQL](https://www.exasol.com/support/browse/SOL-442)
* [Proper csv export from Microsoft SQL Server](https://www.exasol.com/support/browse/SOL-448)

### DB2
See script [db2_to_exasol.sql](db2_to_exasol.sql)
### Exasol
See script [exasol_to_exasol.sql](exasol_to_exasol.sql)
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

### Oracle
When importing from Oracle, you have two options. You could import via JDBC or the  native Oracle interface (OCI).
- OCI: Log in to EXAoperation. Go to *Configuration -> Software*. Download the instant client from Oracle and select it at `Software Update File`. Click `Submit` to upload.

  Create a connection:
  ```SQL
  CREATE CONNECTION <name_of_connection>
  	TO 'jdbc:oracle:thin:@//192.168.99.100:1521/xe'
  	USER '<user>'
    IDENTIFIED BY '<password>';
  ```

- JDBC: If you are using the community edition, you need to upload a JDBC driver in EXAoperation before being able to establish a connection, see [SOL-179](https://www.exasol.com/support/browse/SOL-179).

  Create a connection:
  ``` SQL
  CREATE CONNECTION <name_of_connection>
  	TO '192.168.99.100:1521/xe'
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
The script [s3_to_exasol.sql](s3_to_exasol.sql) looks different than the other import scripts. It's made to load data from S3 in parallel and needs some preparation before you can use it. See [SOL-594](https://www.exasol.com/support/browse/SOL-594) for detailed instructions.
If you just want to import a single file, see 'Import from [CSV](#csv)' above.

### SAP_Hana
The first thing you need to do is add the SAP Hana JDBC driver to Exasol. The JDBC driver is located in your local SAP Hana Installation folder:

* eg: (C:\Program Files\sap\ngdbc.jar) on Microsoft Windows platforms
* eg: (/usr/sap/ngdbc.jar) on Linux and UNIX platforms

In order to add the driver to Exasol log into your EXAOperations, select the 'Software', then 'JDBC Drivers'-Tab.

Click Add then specify the following details:

* Driver Name: SAP (or something similar)
* Main Class: com.sap.db.jdbc.Driver
* Prefix: jdbc:sap:
* Comment: ! no security manager

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. Select the SAP Hana driver by locationg the ngdbc.jar and upload it;
When done the .jar fileshould be listed in the Files column for the SAP Hanadriver.


In order to make your driver accessible in your DBA Visualisation-Software (like DBVisualizer) you have to import the driver via your Driver Manager.
You can find a detailed information about configuring the SAP Hana driver eg. in DBVisualizer at the following link:

https://help.sap.com/viewer/52715f71adba4aaeb480d946c742d1f6/2.0.00/en-US/ff15928cf5594d78b841fbbe649f04b4.html

The standard port number format for different instances 3NN15, NN- represents the instance number of HANA system to be used in client tools. (eg. 31015 Instance No 10, 30015 Instance No 00)
In order to find out the instance number of your Hana-System type the following into your console:
/usr/sap/HXE and press Autocomplete by tab. The Instance-Number should be displayed by the number of the HDB(XX)-File (eg. HDB90)
Insert the number into your port number (-> eg. 39015)

The Connection-String should look like the following: "jdbc:sap://'host_ip':'port'/"
(User-ID: SYSTEM, Password: Your password)


To test the connectivity of Exasol to your SAP Instance create the following connection:

```SQL
CREATE OR REPLACE CONNECTION <name_of_connection>
        TO 'jdbc:sap://<host_name_or_ip_address:port>'
        USER '<td_username>'
        IDENTIFIED BY '<td_password>';
```

You need to have CREATE CONNECTION privilege granted to the user used to do this.

Now, test the connectivity with a simple query like:

```SQL

SELECT *
    FROM   (
               IMPORT FROM JDBC AT HANA_connection
               STATEMENT 'SELECT 1 from dummy'
           );
```
For the actual data-migration, see script [sap_hana_to_exasol.sql](sap_hana_to_exasol.sql)

### SQL Server
See script [sqlserver_to_exasol.sql](sqlserver_to_exasol.sql)

### Teradata
The first thing you need to do is add the Teradata JDBC driver to Exasol. The driver can be downloaded from
[Teradata's Download site](http://downloads.teradata.com/download/connectivity/jdbc-driver). You need to register
first, it's free. Make sure that you download the right version of the JDBC driver, matching the version of the
Teradata database.

The downloaded package contains two files:
* `terajdbc4.jar` contains the actual Java classes of the driver
* `tdgssconfig.jar` includes configuration information for Teradata's Generic Security Services (GSS)

Both files will need to be uploaded when you add the Teradata JDBC driver for Exasol. To do this, log into
EXAoperations, then select _Software_, then the _JDBC Drivers_ tab.

Click ` Add ` then specify the following details:
* Driver Name: `Teradata` (or something similar)
* Main Class: `com.teradata.jdbc.TeraDriver`
* Prefix: `jdbc:teradata:`
* Comment: `Version 15.10` (or something similar)

After clicking ` Apply `, you will see the newly added driver's details on the top section of the driver list.
Select the Teradata driver (the radio button in the first column) and then locate the `terajdbc4.jar` and upload it;
do the same with `tdgssconfig.jar`. When done, both .jar files should be listed in the _Files_ column for the Teradata driver.

Next step is to test the connectivity. First, create a connection to the remote Teradata database:
```SQL
    CREATE OR REPLACE CONNECTION <name_of_connection>
        TO 'jdbc:teradata://<host_name_or_ip_address>'
        USER '<td_username>'
        IDENTIFIED BY '<td_password>';
```
You need to have `CREATE CONNECTION` privilege granted to the user used to do this.
Additional JDBC connection parameters (such as `CHARSET` might need to be specified in the connection string/URL); see information
on these [here](http://developer.teradata.com/doc/connectivity/jdbc/reference/current/frameset.html).

Now, test the connectivity with a simple query:
```SQL
    SELECT *
    FROM   (
               IMPORT FROM JDBC AT <name_of_connection>
               STATEMENT 'SELECT 1'
           );
```
For the actual data-migration, see script [teradata_to_exasol.sql](teradata_to_exasol.sql)

### Vectorwise
See script [vectorwise_to_exasol.sql](vectorwise_to_exasol.sql)

### Vertica
See script [vertica_to_exasol.sql](vertica_to_exasol.sql)

### Google BigQuery
See script [bigquery_to_exasol.sql](bigquery_to_exasol.sql)

Upload the following files into a bucket called "bqmigration" in the default BucketFS service:
- JSON key file for your BigQuery service account
- jar files of the BigQuery JDBC driver (tested with Simba v1.1.6)

The migration script creates DDL and IMPORT statements together with a CREATE CONNECTION statement. In order to perform these imports the JDBC driver has also to be installed in ExaOperation, see [SOL-194](https://www.exasol.com/support/browse/SOL-194) for details.  


## Post-load optimization
This folder contains scripts that can be used after having imported data from another database via the scripts above.
What they do:
- Optimize the column's datatypes to minimize storage space on disk
- Import primary keys from other databases


## Delta import
This folder contains a script that can be used if you want to import data on a regular basis.
What it does:
- Import only data that hasn't been imported yet by performing a delta import based on a given column (further explaination [inside the folder](delta_import))
