# activate virtual env
source venv/bin/activate

# shutdown existing brokers (if any)
docker-compose down

# start kafka broker
docker-compose up -d

# create logs folder if not exist
mkdir -p logs/

# wait for broker to start
sleep 12

# create topics to allow parallelization of producer and consumer w/o topic not exist error
python3 kafka/admin.py

# stream data to kafka and consume
(python3 kafka/producer.py > logs/producer.txt) & python3 kafka/consumer.py
