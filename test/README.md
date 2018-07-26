# Test files for "Database migration" repository

###### Please note that this is an open source project which is *not officially supported* by Exasol. We will try to help you as much as possible, but can't guarantee anything since this is not an official Exasol product.

## Overview

This folder contains the test files needed to automatically test the scripts of the Exasol "Database Migration" repository.
The executable test files are in the subfolder **testing_files**.

## Test structure
Before the tests are executed, an exasol docker container has been built (the name of the container is `exasoldb`) by executing the bash script *exasol_docker.sh*.
Also in this bash script, the *generate_script.sql* file has been executed by the `exasoldb` container. This creates an exasol schema called `database_migration` which will be the schema used for the following tests and a lua script called `generate_script` inside that schema. This lua script is used to parse a *.sql* file and only create the lua scripts written inside that file. We will use the `generate_script` script to create the database migration scripts found in the files we want to test.

The test files have the following names *test_<data_management_system_name>.sh*. For example *test_mysql.sh* or *test_postgres.sh*.

Each test has the same following structure : 
1. Running a container from another data management systems (MySQL, Postgres, SQL-Server, etc.)
2. Creating a database and inserting data within this container (this is the data that we will try to import in an exasol database using the migration scripts). The files which are used for this step are found in the subfolder **testing_files**
3. Creating a connection from the `exasoldb` container to the other container. In this step we create a file *create_conn.sql* and execute it in the `exasoldb` container. 
4. Creating the script corresponding to the data management system. For example, if we ran a mysql container, the script found in the *mysql_to_exasol.sql* file of the database-migration repository will be created. This script is created using the *create_script.py* file. (This python script uses the *generate_script.sql* file to only create the scripts find inside the *mysql_to_exasol.sql* file.)
5. Executing the script created : this step will create the statements to import the all the data from the other data management system into exasol and will write these statements into an *output.sql* file. The script execution is done by the *export_res.py* file.
6. Executing the *output.sql* file by the `exasoldb` container to create the tables and import the data into an exasol database.

If there is any errors in any of these steps, the travis build will break and fail. 
If there is no errors, the build will succeed.

## Example
Let's have a look at the commands found in the *test_mysql.sh* file to illustrate each step of the test structure : 
1. First we have to run a MySQL container, we name it `mysqldb` : 
```
docker run --name mysqldb -p 3360:3306 -e MYSQL_ROOT_PASSWORD=mysql -d mysql:5.7.22
```
2. Then we have to copy the files to create tables and insert data into a MySQL database : 
```
docker cp test/testing_files/mysql_datatypes_test.sql mysqldb:/
docker exec -ti mysqldb sh -c "mysql < mysql_datatypes_test.sql -pmysql"
```
If you want to add tables and data, you can extend this by adding your own *.sql* files in the **testing_files** folder, copy them inside the MySQL container and execute them like shown above.

3. Then we need to link the `exasoldb` container to the `mysqldb` container, to do this we create a connection to the docker container : 
```

ip_mysqldb="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' mysqldb)"
echo "create or replace connection mysql_conn to 'jdbc:mysql://$ip_mysqldb:3306' user 'root' identified by 'mysql';" > test/testing_files/create_conn.sql
docker cp test/testing_files/create_conn.sql exasoldb:/
docker exec -ti exasoldb sh -c "/path/to/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"    
```
4. To create the script inside the *mysql_to_exasol.sql* file, we use the python-exasol package and call the *create_script.py* python script by passing the name of the file as a parameter like so : 
```
PYTHONPATH=<path> python test/create_script.py "mysql_to_exasol.sql"
```
The command above creates the desired script inside the `database_migration` schema. We now want to execute the script created with the desired parameters and save the result of the execution into an *output.sql* file. To do so, we call the *export_res.py* python script like so : 
```
PYTHONPATH=<path> python test/export_res.py "MYSQL_TO_EXASOL" "mysql_conn" "test%" "%"
```
The first parameter is the scriptname, the second is the connection name, the third is the schema filter and the fourth is the table filter.
This python script will create a file called *output.sql* with the corresponding `create table` and `import into` statements.

5. We now want to execute the *output.sql* file in the `exasoldb` container to finally create the tables and insert the data into the `database_migration` schema previously created. We do it with the following commands : 
```
docker cp output.sql exasoldb:/
docker exec -ti exasoldb sh -c "/path/to/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
```