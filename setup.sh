# create and activate virtual env
python3 -m venv venv
source venv/bin/activate

# install required dependencies
pip3 install -r requirements.txt

# initialize oltp and olap databases
python3 setup_dbs.py

# shutdown existing brokers (if any)
docker-compose down

# start kafka broker
docker-compose up -d

# wait for services to start properly and register mysql database to kafka connect
sleep 60 && python3 kafka_connect/register_mysql.py
