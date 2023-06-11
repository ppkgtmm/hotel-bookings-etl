import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text
import time

load_dotenv()

server_key = "KAFKA_BOOTSTRAP_SERVERS"
db_password_key = "DB_PASSWORD"

db_host = "localhost"
db_user = "root"
db_password = getenv(db_password_key)
db_name = "olap_hotel"

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_name}"
)

conf = {
    "bootstrap.servers": getenv(server_key),
    "group.id": "olap_staging",
    "auto.offset.reset": "earliest",
}

oltp_tables = [
    "users",
    "guests",
    "addons",
    "roomtypes",
    "rooms",
    "bookings",
    "booking_rooms",
    "booking_room_addons",
]


def stage_data():
    try:
        start = time.time()
        while True and (time.time() - start) <= 60:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                break
            else:
                topic = msg.topic()
                value = json.loads(msg.value())
                conn.execute(text(queries[topic]), value)
                conn.commit()

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()
    consumer = Consumer(conf)
    consumer.subscribe(oltp_tables)
    queries = {}
    for table_name in oltp_tables:
        columns = conn.execute(text(f"SHOW COLUMNS FROM stg_{table_name}"))
        columns = [column[0] for column in columns]
        query = f"""INSERT INTO stg_{table_name} ({', '.join(columns)}) VALUES ({', '.join([':'+col for col in columns])})
                    ON DUPLICATE KEY UPDATE {', '.join([col+'='+':'+col for col in columns])}
                """
        queries[table_name] = query
    stage_data()
    conn.close()
    engine.dispose()
