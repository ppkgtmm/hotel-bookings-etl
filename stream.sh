# activate virtual env
source venv/bin/activate

# create logs folder if not exist
mkdir -p logs/

# stream oltp data to kafka and consume i.e stage data in olap database
(python3 kafka/producer.py > logs/producer.txt) & python3 kafka/consumer.py
