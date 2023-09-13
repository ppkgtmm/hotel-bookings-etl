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
	${PYTHON} dbs/initialize.py

# generate fake booking data
datagen:
	${PYTHON} datagen/generate.py

# seed OLTP database
populate:
	${PYTHON} dbs/populate.py    

# register OLTP database to kafka connect
kconnect:
	${PYTHON} connect/register_source.py

# load data to date dimension table
dim-date:
	${PYTHON} scripts/dim_date.py
