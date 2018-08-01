#!/bin/bash
MY_MESSAGE="Starting test exasol!"
echo $MY_MESSAGE

set -e

#setting up an exasol db image in docker
docker pull exasol/docker-db:latest
# to run locally : docker run --name exasoldb -p 8877:8888 --detach --privileged --stop-timeout 120  exasol/docker-db:latest
docker run --name exasoldb2 -p 127.0.0.1:8877:8888 --detach --privileged --stop-timeout 120  exasol/docker-db:latest

# Wait until database is ready
(docker logs -f --tail 0 exasoldb2 &) 2>&1 | grep -q -i 'stage4: All stages finished'
sleep 60


docker cp test/testing_files/retail_mini/ exasoldb2:/
docker exec -ti exasoldb2 sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.10/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "retail_mini/retail_mini.sql" -x"

#find the ip address of the exasol container
ip="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' exasoldb2)"
echo "create or replace connection SECOND_EXASOL_DB to '$ip:8888' user 'sys' identified by 'exasol';" > test/testing_files/create_conn.sql

#copy .sql file to be executed inside container
docker cp test/testing_files/create_conn.sql exasoldb:/
#execute the file inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.10/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"

#create the script that we want to execute
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/create_script.py "exasol_to_exasol.sql"
#this python script executes the export script created by the exasol_to_exasol.sql script and creates an output.sql file with the result
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/export_res.py "EXASOL_TO_EXASOL" "SECOND_EXASOL_DB" "RET%" "%"

#delete previous output.sql file if exists : 
file="output.sql"
docker exec -ti exasoldb sh -c "[ ! -e $file ] || rm $file"
#copy new output.sql file to be executed inside container
docker cp $file exasoldb:/
#execute the output.sql file created inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.10/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
#delete the file from current directory
[ ! -e $file ] || rm $file

#stop and remove exasoldb2 container
docker stop exasoldb2
docker rm -v exasoldb2