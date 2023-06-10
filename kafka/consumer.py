import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text

load_dotenv()

min_commit_count = 4
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
topic = "hotel_oltp_data"
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
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                break
            if msg.error():
                print(msg.error())
                break
            else:
                topic = msg.topic()
                conn.execute(text(queries[topic]), json.loads(msg.value()))
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
        query = f"INSERT INTO stg_{table_name} ({', '.join(columns)}) VALUES ({', '.join([':'+col for col in columns])})"
        queries[table_name] = query
    stage_data()
    conn.close()
    engine.dispose()
