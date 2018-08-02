#!/bin/bash
MY_MESSAGE="Starting test postgres!"
echo $MY_MESSAGE

set -e

#setting up a postgres db image in docker and running a container
docker pull postgres:latest
docker run --name postgresdb -p 5423:5432 -e POSTGRES_PASSWORD=postgres -d postgres
#wait until the postgresdb container if fully initialized
(docker logs -f --tail 0 postgresdb &) 2>&1 | grep -q -i 'database system is ready to accept connections'
sleep 20

#copy .sql file to be executed inside container
docker cp test/testing_files/postgres_datatypes_test.sql postgresdb:/tmp/
#execute the file inside the postgresdb container
docker exec -u postgres postgresdb psql postgres postgres -f tmp/postgres_datatypes_test.sql


#find the ip address of the postgres container
ip="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' postgresdb)"
echo "create or replace connection postgres_db to 'jdbc:postgresql://$ip:5432/postgres' user 'postgres' identified by 'postgres';" > test/testing_files/create_conn.sql

#copy .sql file to be executed inside container
docker cp test/testing_files/create_conn.sql exasoldb:/
#execute the file inside the exasoldb container
docker exec -ti exasoldb sh -c "$exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"


#create the script that we want to execute
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/create_script.py "postgres_to_exasol.sql"
#this python script executes the export script created by the postgres_to_exasol.sql script and creates an output.sql file with the result
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/export_res.py "POSTGRES_TO_EXASOL" "postgres_db" "%test%" "%"

 
file="output.sql"
#delete previous output.sql file if exists inside container:
docker exec -ti exasoldb sh -c "[ ! -e $file ] || rm $file"
#copy new output.sql file to be executed inside container
docker cp $file exasoldb:/
#execute the output.sql file created inside the exasoldb container
docker exec -ti exasoldb sh -c "$exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
#delete the file from current directory
[ ! -e $file ] || rm $file

#stop and remove postgresdb container
docker stop postgresdb
docker rm -v postgresdb