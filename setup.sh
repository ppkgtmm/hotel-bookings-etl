# create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# install required dependencies
pip3 install -r requirements.txt

# start required containers
docker-compose up -d mysql zookeeper broker kafka-connect

# wait for containers to start properly
sleep 60

# initialize oltp and olap databases
python3 setup_dbs.py

# create required kafka topics
python3 kafka-admin.py
