# Database migration
[![Build Status](https://travis-ci.org/exasol/database-migration.svg?branch=master)](https://travis-ci.org/exasol/database-migration)

###### Please note that this is an open source project which is *not officially supported* by Exasol. We will try to help you as much as possible, but can't guarantee anything since this is not an official Exasol product.

## Table of Contents
1. [Overview](#overview)
2. [Migration source:](#migration-source)
    * [Azure Sql](#azure-sql)
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

### Azure SQL
Azure SQL is essentially Microsoft SQL Server. You need to specify a DB you are working on in your connection-string.
See script [azure_sql_to_exasol.sql](azure_sql_to_exasol.sql)


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

For more details on `IMPORT` see paragraph 2.2.2 in the User Manual. For further help on typical CSV-formatting issues, see
* [How to load bad CSV files](https://exasol.my.site.com/s/article/How-to-load-bad-CSV-files?language=en_US)
* [Proper csv export from MySQL](https://exasol.my.site.com/s/article/Proper-csv-export-from-MySQL?language=en_US)
* [Proper csv export from IBM DB2](https://exasol.my.site.com/s/article/Proper-csv-export-from-IBM-DB2?language=en_US)
* [Proper csv export from Oracle](https://exasol.my.site.com/s/article/Proper-csv-export-from-Oracle?language=en_US)
* [Proper csv export from PostgreSQL](https://exasol.my.site.com/s/article/Proper-csv-export-from-PostgreSQL?language=en_US)
* [Proper csv export from Microsoft SQL Server](https://exasol.my.site.com/s/article/Proper-csv-export-from-Microsoft-SQL-Server?language=en_US)

### DB2
See script [db2_to_exasol.sql](db2_to_exasol.sql)
### Exasol
Step by Step guide:
* create connection to the Exasol database you want to import from
* create the [exasol_to_exasol.sql](exasol_to_exasol.sql) script
* adapt the variables for the execute script accordingly to your scenario and run the statement
  * depending on your system and the amount of tables this might take a few seconds
* copy the result set to another session and execute the statement in the output order

This script will generate the following information:
* create schema
* create table with primary keys
* alter table add foreign keys
* alter table set partion by 
* alter table set distribution keys
* import data

See script [exasol_to_exasol.sql](exasol_to_exasol.sql) for more information!

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


**Configure the Driver in EXAoperation**

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

In order to add the driver to Exasol log into your EXAOperations, select the 'Software'-, then 'JDBC Drivers'-Tab.

Click Add, then specify the following details:

* Driver Name: `Netezza`
* Main Class: `org.netezza.Driver`
* Prefix: `jdbc:netezza:`
* Disable Security Manager: `Check this box`

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. 
Select the Netezza driver by locating the nzjdbc3.jar and upload it. When done the .jar file should be listed in the files column for the IBM Netezza driver.

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
- OCI: Log in to EXAoperation. Go to *Configuration -> Software*. Download the instant client from Oracle and select it at `Software Update File`. Click `Submit` to upload, see [Oracle OCI](https://docs.exasol.com/db/latest/loading_data/connect_sources/oracle.htm#OracleOCI).

  Create a connection:
  ``` SQL
  CREATE CONNECTION <name_of_connection>
  	TO '192.168.99.100:1521/xe'
    USER '<user>'
    IDENTIFIED BY '<password>';
  ```

- JDBC: If you are using the community edition, you need to upload a JDBC driver in EXAoperation before being able to establish a connection, see [Oracle JDBC](https://docs.exasol.com/db/latest/loading_data/connect_sources/oracle.htm#OracleJDBC).

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

In order to add the driver to Exasol log into your EXAOperations, select the 'Software', then 'JDBC Drivers'-Tab.

Click Add then specify the following details:

* Driver Name: `SAP`
* Main Class: `com.sap.db.jdbc.Driver`
* Prefix: `jdbc:sap:`
* Disable Security Manager: `Check this box`

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. Select the SAP Hana driver by locating the ngdbc.jar and upload it.
When done the .jar file should be listed in the files column for the SAP Hana driver.


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

In order to add the driver to Exasol log into your EXAOperations, select the 'Software', then 'JDBC Drivers'-Tab.

Click Add then specify the following details:

* Driver Name: `Snowflake`
* Main Class: `net.snowflake.client.jdbc.SnowflakeDriver`
* Prefix: `jdbc:snowflake:`
* Disable Security Manager: `Check this box`

After clicking Apply, you will see the newly added driver's details on the top section of the driver list. Select the Snowflake driver by locating the corresponding jar and upload it. When done the .jar file should be listed in the files column for the Snowflake driver.

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
See script [sqlserver_to_exasol.sql](sqlserver_to_exasol.sql)

### Teradata
The first thing you need to do is add the Teradata JDBC driver to Exasol. The driver can be downloaded from
[Teradata's Download site](https://downloads.teradata.com/download/connectivity/jdbc-driver). You need to register
first, it's free. Make sure that you download the right version of the JDBC driver, matching the version of the
Teradata database.

The downloaded package contains the file:
* `terajdbc4.jar` contains the actual Java classes of the driver

This file needs to be uploaded when you add the Teradata JDBC driver for Exasol. To do this, log into
EXAoperations, then select _Software_, then the _JDBC Drivers_ tab.

Click ` Add ` then specify the following details:
* Driver Name: `Teradata` (or something similar)
* Main Class: `com.teradata.jdbc.TeraDriver`
* Prefix: `jdbc:teradata:`
* Comment: `Version 15.10` (or something similar)

After clicking ` Apply `, you will see the newly added driver's details on the top section of the driver list.
Select the Teradata driver (the radio button in the first column) and then locate the `terajdbc4.jar` and upload it.

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


## Post-load optimization
This folder contains scripts that can be used after having imported data from another database via the scripts above.
What they do:
- Optimize the column's datatypes to minimize storage space on disk
- Import primary keys from other databases


## Delta import
This folder contains a script that can be used if you want to import data on a regular basis.
What it does:
- Import only data that hasn't been imported yet by performing a delta import based on a given column (further explaination [inside the folder](delta_import))
