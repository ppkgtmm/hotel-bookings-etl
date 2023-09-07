# create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# install required dependencies
pip3 install -r requirements.txt

# start required containers
docker-compose up -d mysql zookeeper broker kafka-connect schema-registry ksqldb-server

# wait for containers to start properly
sleep 60

# initialize oltp and olap databases
python3 setup_dbs.py
