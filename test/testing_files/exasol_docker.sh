#!/bin/bash
MY_MESSAGE="Starting exasol docker!"
echo $MY_MESSAGE

set -e

#setting up an exasol db image in docker
docker pull exasol/docker-db:latest
# to run locally : docker run --name exasoldb -p 8899:8888 --detach --privileged --stop-timeout 120  exasol/docker-db:latest
docker run --name exasoldb -p 127.0.0.1:8899:8888 --detach --privileged --stop-timeout 120  exasol/docker-db:latest

# Wait until database is ready
(docker logs -f --tail 0 exasoldb &) 2>&1 | grep -q -i 'stage4: All stages finished'
sleep 60

#copy the generate_script.sql file
docker cp test/generate_script.sql exasoldb:/
#execute the generate_script.sql file which creates a script inside the exasoldb container
docker exec -ti exasoldb sh -c "/usr/opt/EXASuite-6/EXASolution-6.0.10/bin/Console/exaplus  -c "127.0.0.1:8888" -u sys -p exasol -f "generate_script.sql" -x"
