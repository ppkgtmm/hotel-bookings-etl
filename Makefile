PYTHON = ./venv/bin/python3
PIP = ./venv/bin/pip3

####################################################################################

install:
	python3 -m venv venv && ${PIP} install -r requirements.txt

up:
	docker-compose up -d mysql zookeeper && \
	sleep 60 && \
	docker-compose up -d broker schema-registry kafka-connect --no-recreate

down:
	docker compose down -v

db-init:
	${PYTHON} dbs/initialize.py

setup: install down up db-init

####################################################################################

data-gen:
	${PYTHON} datagen/generate.py

populate:
	${PYTHON} dbs/populate.py

seed-oltp: data-gen populate

####################################################################################

kconnect:
	${PYTHON} connect/register_source.py

dim-date:
	${PYTHON} scripts/dim_date.py

run: kconnect dim-date

####################################################################################
