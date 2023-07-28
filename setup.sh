# create and activate virtual env
python3 -m venv venv
source venv/bin/activate

# install required dependencies
pip3 install -r requirements.txt

# start containers
docker-compose up -d

# wait for services to start properly
sleep 45

# initialize oltp and olap databases
python3 setup_dbs.py
