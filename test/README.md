# Test files for "Database migration" repository

###### Please note that this is an open source project which is *not officially supported* by Exasol. We will try to help you as much as possible, but can't guarantee anything since this is not an official Exasol product.

## Overview

This folder contains the test files needed to automatically test the scripts of the Exasol "Database Migration" repository.
The executable bash test files are in the subfolder **testing_files**.

## Test structure
Before the tests are executed, an exasol docker container has been built and is running in detached mode (the name of the container is `exasoldb`) by executing the bash script *exasol_docker.sh*.
Also in this bash script, the *generate_script.sql* file has been executed by the `exasoldb` container. This creates an exasol schema called `database_migration` which will be the schema used for the following tests and a Lua script called `generate_script` inside that schema. This Lua script is used to parse a *.sql* file and only create the Lua scripts written inside that file. We will use the `generate_script` script to create the database migration scripts found in the files we want to test.

The test files names follow this syntax : *test_<data_management_system_name>.sh*. For example *test_mysql.sh* or *test_postgres.sh*.

Each test follows the same structure, consisted by the following steps : 
1. Running a docker container from another data management systems (MySQL, Postgres, SQL-Server, etc.)
2. Creating a database and inserting data within this container (this is the data that we will try to import into an exasol database using the migration scripts). The files which are used for this step are found in the subfolder **testing_files**.
3. Creating a connection from the `exasoldb` container to the other container. In this step we create a file *create_conn.sql* which contains the `create connection` statement and execute it in the `exasoldb` container.
4. Creating the script corresponding to the data management system we want to test. For example, if we ran a mysql container, the script found in the *mysql_to_exasol.sql* file of the database-migration repository will be created. This script is created using the *create_script.py* file. (This python script uses the `GENERATE_SCRIPT` lua script previously created to only create the scripts found inside the *mysql_to_exasol.sql* file.)
5. Executing the script created : during this step, the statements to create the tables and import all the data from the other data management system into exasol are creeated and written into an *output.sql* file. The script execution is done using the *export_res.py* file. This way, we can pass the arguments that will be used during the execution of the script.
6. Executing the *output.sql* file by the `exasoldb` container to create the tables and import the data into the exasol `database_migration` schema.

If there is any errors in any of these steps, the travis build will break and fail. 
If there is no errors - which means that the migration was successful - the build will pass.

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
If you want to add tables and data to the tests, you can extend this step by adding your own *.sql* files in the **testing_files** folder. Then you can copy them inside the MySQL container and execute them like shown above.

3. Then we need to link the `exasoldb` container to the `mysqldb` container. To do this we create the statement for the connection by using the IP adress of the mysql docker container and write this statement in a *.sql* file, then we execute this file in the `exasoldb` container : 
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
The command above creates the desired script inside the `database_migration` schema. In our example, we created the `MYSQL_TO_EXASOL` script. 

5. After the creation, we want to execute the script created with the desired parameters and save the result of the execution into an *output.sql* file. To do so, we call the *export_res.py* python script like so : 
```
PYTHONPATH=<path> python test/export_res.py "MYSQL_TO_EXASOL" "mysql_conn" "test%" "%"
```
The first parameter is the scriptname, the second is the connection name, the third is the schema filter and the fourth is the table filter.
This python script will create a file called *output.sql* with the corresponding `create table` and `import into` statements.

6. We now want to execute the *output.sql* file in the `exasoldb` container to finally create the tables and insert the data into the `database_migration` schema previously created. We do it with the following commands : 
```
docker cp output.sql exasoldb:/
docker exec -ti exasoldb sh -c "/path/to/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
```
After this step, if all the previous steps exited with no errors, the data that was found inside the `test` MySQL database has been successfully inserted into our `database_migration` schema - which means the build is passing.