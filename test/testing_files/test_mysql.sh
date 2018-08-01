#!/bin/bash
MY_MESSAGE="Starting test mysql!"
echo $MY_MESSAGE

set -e

#setting up a mysql db image in docker
docker pull mysql:5.7.22
docker run --name mysqldb -p 3360:3306 -e MYSQL_ROOT_PASSWORD=mysql -d mysql:5.7.22
#wait until the postgresdb container if fully initialized
(docker logs -f --tail 0 mysqldb &) 2>&1 | grep -q -i 'port: 3306  MySQL Community Server (GPL)'

#copy .sql file to be executed inside container
docker cp test/testing_files/mysql_datatypes_test.sql mysqldb:/tmp/
#execute the file inside the mysqldb container
docker exec -ti mysqldb sh -c "mysql < tmp/mysql_datatypes_test.sql -pmysql"

#find the ip address of the mysql container
ip="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' mysqldb)"
echo "create or replace connection mysql_conn to 'jdbc:mysql://$ip:3306' user 'root' identified by 'mysql';" > test/testing_files/create_conn.sql

#copy .sql file to be executed inside container
docker cp test/testing_files/create_conn.sql exasoldb:/
#execute the file inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.11/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"




#create the script that we want to execute
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/create_script.py "mysql_to_exasol.sql"
#this python script executes the export script created by the mysql_to_exasol.sql script and creates an output.sql file with the result
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/export_res.py "MYSQL_TO_EXASOL" "mysql_conn" "testing_d%" "%"

#delete previous output.sql file if exists : 
file="output.sql"
docker exec -ti exasoldb sh -c "[ ! -e $file ] || rm $file"
#copy new output.sql file to be executed inside container
docker cp $file exasoldb:/
#execute the output.sql file created inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.11/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
#delete the file from current directory
[ ! -e $file ] || rm $file

#stop and remove mysqldb container
docker stop mysqldb
docker rm -v mysqldb