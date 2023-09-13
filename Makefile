PYTHON = ./venv/bin/python3
PIP = ./venv/bin/pip3

# create virtual environment
setup:
	python3 -m venv venv

# install required dependencies
install: setup
	${PIP} install -r requirements.txt

# start containers required for project
up:
	docker-compose up -d mysql zookeeper && \
	sleep 60 && \
	docker-compose up -d broker schema-registry kafka-connect --no-recreate

# tear down containers
down:
	docker compose down -v

# initialize oltp and olap databases
db-init:
	python3 dbs/initialize.py

# combination of steps required for project set up
setup: create-venv activate install down up db-init

# generate fake booking data
datagen:
	python3 datagen/generate.py

# seed OLTP database
populate:
	python3 dbs/populate.py    

# register OLTP database to kafka connect
kconnect:
	python3 connect/register_source.py

# load data to date dimension table
dim-date:
	python3 scripts/dim_date.py
