import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text
import time

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
oltp_db = getenv("OLTP_DB")
olap_db = getenv("OLAP_DB")

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{olap_db}"
)

conf = {
    "bootstrap.servers": getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "group.id": "olap_staging",
    "auto.offset.reset": "earliest",  # start reading from first offset if no commits yet
}

oltp_tables = [
    "users",
    "guests",
    "addons",
    "roomtypes",
    "rooms",
    "bookings",
    "booking_rooms",
    "booking_addons",
]


def consume_data(consumer):
    try:
        start = time.time()
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                break
            else:
                topic = msg.topic()
                value = json.loads(msg.value())["payload"]["after"]
                conn.execute(text(queries[topic]), value)
                conn.commit()
            print("inserted record,", time.time() - start, "seconds passed")

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()
    consumer = Consumer(conf)
    topics = ["{}.{}.{}".format(*([oltp_db] * 2), table) for table in oltp_tables]
    consumer.subscribe(topics)
    queries = {}
    for table_name, topic in zip(oltp_tables, topics):
        columns = conn.execute(text(f"SHOW COLUMNS FROM stg_{table_name}"))
        columns = [(column[0], column[1].decode("utf-8")) for column in columns]
        values = []
        for col, dtype in columns:
            processor = ":{}"
            if dtype == "date":
                processor = "CAST(:{} AS DATE)"
            elif dtype == "datetime":
                processor = "CAST(:{} AS DATETIME)"
            elif dtype == "timestamp":
                processor = "TIMESTAMP(STR_TO_DATE(:{}, '%Y-%m-%dT%H:%i:%sZ'))"
            values.append(processor.format(col))
        query = f"""INSERT INTO stg_{table_name} ({', '.join([col[0] for col in columns])}) VALUES ({', '.join(values)})
                    ON DUPLICATE KEY UPDATE {', '.join([col[0]+'='+val for col, val in zip(columns, values)])}
                """
        queries[topic] = query
    consume_data(consumer)
    conn.close()
    engine.dispose()
