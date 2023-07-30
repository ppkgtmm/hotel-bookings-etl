# create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# install required dependencies
pip3 install -r requirements.txt

# start required containers
docker-compose up -d

# wait for containers to start properly
sleep 45

# initialize oltp and olap databases
python3 setup_dbs.py

# register OLTP database to kafka connect
python3 kafka_connect/register_mysql.py
