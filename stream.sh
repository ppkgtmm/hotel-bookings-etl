# activate virtual env
source venv/bin/activate

# shutdown existing brokers (if any)
docker-compose down

# start kafka broker
docker-compose up -d

# create logs folder if not exist
mkdir -p logs/

python3 kafka/producer.py > logs/producer.txt & \
(sleep 3.5 && python3 kafka/consumer.py)
