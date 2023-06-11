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

# wait for broker to start
sleep 12

# create topics to allow parallelization of producer and consumer w/o topic not exist error
python3 kafka/admin.py
