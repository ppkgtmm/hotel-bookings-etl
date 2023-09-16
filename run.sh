#!/bin/sh

GREEN="\033[0;32m"
RED="\033[0;31m"
CLEAR="\033[0m"

activate() {
    source venv/bin/activate
}

down() {
    docker compose down -v
}

print() {
 echo "${GREEN}$1${CLEAR}"
}

setup() {
    print "running project set up"
    python3 -m venv venv
    activate
    pip3 install -r requirements.txt
    down
    docker-compose up -d mysql zookeeper
	sleep 60
	docker-compose up -d broker schema-registry kafka-connect --no-recreate
    python3 dbs/initialize.py
    print "done setting up project"
}

datagen() {
    activate
    python3 datagen/generate.py
    print "done generating fake booking data"

}	

seed() {
    activate
    python3 dbs/populate.py
    print "done seeding oltp database"
}

etl() {
    print "registering oltp db and running ETL"
    activate
    python3 connect/register_source.py
    python3 scripts/dimensions/dim_date.py
    python3 scripts/dimensions/dim_location.py
}

usage() {  
    echo "usage: ./run.sh command"  
    echo "where command is one of setup, datagen, seed, etl, down"
} 

if [ "$1" = "setup" ]
then
    setup
elif [ "$1" = "datagen" ]
then
    datagen
elif [ "$1" = "seed" ]
then
    seed
elif [ "$1" = "etl" ]
then
    etl
elif [ "$1" = "down" ]
then
    down   
else
    usage
    echo "${RED}error : invalid argument${CLEAR}"
    exit 1
fi
