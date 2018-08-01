#!/bin/bash
MY_MESSAGE="Starting test DB2!"
echo $MY_MESSAGE

set -e

curl -L $DB2_PATH -o db2.tar.gz
tar -xzf db2.tar.gz

curl -L $DB1_CFG -o db1.cfg
docker cp db1.cfg exasoldb:/
docker exec -ti exasoldb sh -c "dwad_client stop-wait DB1; dwad_client setup DB1 db1.cfg; dwad_client start DB1"

docker pull ibmcom/db2express-c
docker run --name db2db -d -p 50000:50000 -e DB2INST1_PASSWORD=test123 -e LICENSE=accept  ibmcom/db2express-c:latest db2start
#wait until the db2db container if fully initialized
(docker logs -f --tail 0 db2db &) 2>&1 | grep -q -i 'DB2START processing was successful.'

#create a sample database (necessary to use db2)
docker exec -it db2db bin/bash -c "su - db2inst1 -c \"db2 create db sample\""

#copy .sql file to be executed inside container
docker cp test/testing_files/db2_datatypes_test.sql db2db:/home/db2inst1/	
#execute the file inside the db2db container
docker exec -it db2db bin/bash -c "su - db2inst1 -c \"db2 -stvf db2_datatypes_test.sql\""

#find the ip address of the db2 container
ip="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' db2db)"
echo "create or replace CONNECTION db2_connection TO 'jdbc:db2://$ip:50000/sample' USER 'db2inst1' IDENTIFIED BY 'test123';" > test/testing_files/create_conn.sql


#copy .sql file to be executed inside container
docker cp test/testing_files/create_conn.sql exasoldb:/
#execute the file inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.11/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"


docker cp exasoldb:/exa/etc/EXAConf .
pwd="$(awk '/WritePasswd/{ print $3; }' EXAConf | base64 -d)"

docker cp db2 exasoldb:/db2
docker exec -ti exasoldb sh -c "curl -v -X PUT -T db2/settings.cfg http://w:$pwd@127.0.0.1:6583/default/drivers/jdbc/db2/"
docker exec -ti exasoldb sh -c "curl -v -X PUT -T db2/db2jcc4.jar http://w:$pwd@127.0.0.1:6583/default/drivers/jdbc/db2/"
docker exec -ti exasoldb sh -c "curl -v -X PUT -T db2/LICENSE.txt http://w:$pwd@127.0.0.1:6583/default/drivers/jdbc/db2/"
sleep 20


#create the script that we want to execute
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/create_script.py "db2_to_exasol.sql"
#this python script executes the export script created by the db2_to_exasol.sql script and creates an output.sql file with the result
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/export_res.py "DB2_TO_EXASOL" "DB2_CONNECTION" "%TESTI%" "%"

#delete previous output.sql file if exists : 
file="output.sql"
docker exec -ti exasoldb sh -c "[ ! -e $file ] || rm $file"
#copy new output.sql file to be executed inside container
docker cp $file exasoldb:/
#execute the output.sql file created inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.11/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
#delete the file from current directory
[ ! -e $file ] || rm $file
#stop and remove the db2 container
docker stop db2db
docker rm -v db2db