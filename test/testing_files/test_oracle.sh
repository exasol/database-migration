#!/bin/bash
MY_MESSAGE="Starting test Oracle!"
echo $MY_MESSAGE

set -e

curl -L $OCI_PATH -o instantclient-basic-linux.x64-12.1.0.2.0.zip

docker pull sath89/oracle-12c:novnc
docker run --name oracledb -d -p 8080:8080 -p 1521:1521 sath89/oracle-12c:novnc
#wait until the oracledb container if fully initialized
(docker logs -f --tail 0 oracledb &) 2>&1 | grep -q -i 'Database ready to use. Enjoy! ;)'

#copy .sql file to be executed inside container
docker cp test/testing_files/oracle_datatypes_test.sql oracledb:/tmp/
docker exec -ti oracledb sh -c "echo exit | sqlplus -S system/oracle@localhost:1521/xe @/tmp/oracle_datatypes_test.sql"


#find the ip address of the oracle container
ip="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' oracledb)"
echo "CREATE OR REPLACE CONNECTION OCI_ORACLE TO '$ip:1521/xe' USER 'system' IDENTIFIED BY 'oracle';" > test/testing_files/create_conn.sql

docker cp exasoldb:/exa/etc/EXAConf .
pwd="$(awk '/WritePasswd/{ print $3; }' EXAConf | base64 -d)"

docker cp instantclient-basic-linux.x64-12.1.0.2.0.zip exasoldb:/
docker exec -ti exasoldb sh -c "curl -v -X PUT -T instantclient-basic-linux.x64-12.1.0.2.0.zip http://w:$pwd@127.0.0.1:6583/default/drivers/oracle/instantclient-basic-linux.x64-12.1.0.2.0.zip"
sleep 20

#copy .sql file to be executed inside container
docker cp test/testing_files/create_conn.sql exasoldb:/
#execute the file inside the exasoldb container
docker exec -ti exasoldb sh -c "$exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"

#create the script that we want to execute
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/create_script.py "oracle_to_exasol.sql"
#this python script executes the export script created by the oracle_to_exasol.sql script and creates an output.sql file with the result
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/export_res.py "ORACLE_TO_EXASOL" "OCI_ORACLE" "EXA%" "%"

#delete previous output.sql file if exists : 
file="output.sql"
docker exec -ti exasoldb sh -c "[ ! -e $file ] || rm $file"
#copy new output.sql file to be executed inside container
docker cp $file exasoldb:/
#execute the output.sql file created inside the exasoldb container
docker exec -ti exasoldb sh -c "$exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
#delete the file from current directory
[ ! -e $file ] || rm $file

#stop and remove container
docker stop oracledb
docker rm -v oracledb