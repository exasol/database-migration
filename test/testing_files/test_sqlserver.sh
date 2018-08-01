#!/bin/bash
MY_MESSAGE="Starting test sqlserver!"
echo $MY_MESSAGE

set -e

#setting up a sqlserver db image in docker and running a container
docker pull microsoft/mssql-server-linux:2017-latest
docker run --name sqlserverdb -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=my_strong_Password' -p 1433:1433 -d microsoft/mssql-server-linux:2017-latest
#wait until the sqlserverdb container if fully initialized
(docker logs -f --tail 0 sqlserverdb &) 2>&1 | grep -q -i 'SQL Server is now ready for client connections.'

#copy .sql file to be executed inside container
docker cp test/testing_files/sqlserver_datatypes_test.sql sqlserverdb:/tmp/
#execute the file inside the sqlserver container
docker exec -ti sqlserverdb sh -c "/opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U SA -P 'my_strong_Password' -i tmp/sqlserver_datatypes_test.sql"

#find the ip address of the sqlserver container
ip="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' sqlserverdb)"
echo "create or replace connection sqlserver_connection TO 'jdbc:jtds:sqlserver://$ip:1433' user 'sa' identified by 'my_strong_Password';" > test/testing_files/create_conn.sql

#copy .sql file to be executed inside container
docker cp test/testing_files/create_conn.sql exasoldb:/
#execute the file inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.11/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "create_conn.sql" -x"


#create the script that we want to execute
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/create_script.py "sqlserver_to_exasol.sql"
#this python script executes the export script created by the sqlserver_to_exasol.sql script and creates an output.sql file with the result
PYTHONPATH=$HOME/exa_py/lib/python2.7/site-packages python test/export_res.py "SQLSERVER_TO_EXASOL" "sqlserver_connection" "testing" "dbo" "%" false

 
file="output.sql"
#delete previous output.sql file if exists inside container:
docker exec -ti exasoldb sh -c "[ ! -e $file ] || rm $file"
#copy new output.sql file to be executed inside container
docker cp $file exasoldb:/
#execute the output.sql file created inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.11/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "output.sql" -x"
#delete the file from current directory
[ ! -e $file ] || rm $file

#stop and remove sqlserverdb container
docker stop sqlserverdb
docker rm -v sqlserverdb