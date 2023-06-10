import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
import asyncio

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


async def stage_data(table_name: str, conn: Connection):
    try:
        consumer = Consumer(conf)
        consumer.subscribe([table_name])
        msg_count = 0
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    print(
                        f"{msg.topic()}: {msg.partition()} reached end at offset {msg.offset()}"
                    )
                    break
                raise KafkaException(msg.error())
            else:
                result = msg.value().decode("utf-8")
                conn.execute(
                    text(
                        f"INSERT INTO stg_{table_name} VALUES ({','.join('?'*len(result))})",
                        result,
                    )
                )
                msg_count += 1
                if msg_count % min_commit_count == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


async def main():
    engine = create_engine(connection_string)
    conn = engine.connect()
    tasks = [asyncio.create_task(stage_data(table, conn)) for table in oltp_tables]
    await asyncio.gather(*tasks)
    conn.close()
    engine.dispose()


asyncio.run(main())
