#!/bin/sh

GREEN="\033[0;32m"
RED="\033[0;31m"
CLEAR="\033[0m"

set -e # exit immediately if any command fails

activate_venv() {
    source venv/bin/activate
}

tear_down() {
    docker compose down -v
}

print() {
 echo "${GREEN}$1${CLEAR}"
}

setup() {
    print "running project set up"
    python3.9 -m venv venv
    activate_venv
    pip3 install -r requirements.txt
    tear_down
    docker-compose up -d mysql zookeeper
	sleep 60
	docker-compose up -d broker schema-registry kafka-connect --no-recreate
    python3 dbs/initialize.py
    print "done setting up project"
}

generate_data() {
    activate_venv
    python3 datagen/generate.py
    print "done generating fake booking data"

}	

seed_oltp() {
    activate_venv
    python3 dbs/populate.py
    print "done seeding oltp database"
}

etl() {
    print "registering oltp db and running ETL"
    activate_venv
    python3 connect/register_source.py
    python3 batch/dependencies/dim_date.py
    python3 batch/dependencies/dim_location.py
    docker-compose up -d stream-processor --no-recreate 
}

start_airflow() {
    docker-compose up -d airflow-scheduler airflow-webserver --no-recreate
}

usage() {  
    echo "usage: ./run.sh command"  
    echo "where command is one of setup, datagen, seed, etl, airflow and down"
} 

if [ "$1" = "setup" ]
then
    setup
elif [ "$1" = "datagen" ]
then
    generate_data
elif [ "$1" = "seed" ]
then
    seed_oltp
elif [ "$1" = "etl" ]
then
    etl
elif [ "$1" = "airflow" ]
then
    start_airflow
elif [ "$1" = "down" ]
then
    tear_down
else
    usage
    echo "${RED}error : invalid argument${CLEAR}"
    exit 1
fi
