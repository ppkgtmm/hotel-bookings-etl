# create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# install required dependencies
pip3 install -r requirements.txt

# start mysql and zookeeper containers
docker-compose up -d mysql zookeeper

# wait for containers to start properly
sleep 60

# start kafka broker container
docker-compose up -d broker --no-recreate

# initialize oltp and olap databases
python3 setup_dbs.py

# start other required containers
docker-compose up -d schema-registry kafka-connect ksqldb-server
