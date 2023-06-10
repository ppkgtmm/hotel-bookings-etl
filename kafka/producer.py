from confluent_kafka import Producer
from dotenv import load_dotenv
from os import getenv
import socket
from sqlalchemy import create_engine, text
import asyncio

load_dotenv()

server_key = "KAFKA_BOOTSTRAP_SERVERS"
db_password_key = "DB_PASSWORD"

db_host = "localhost"
db_user = "root"
db_password = getenv(db_password_key)
db_name = "oltp_hotel"

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_name}"
)

conf = {"bootstrap.servers": getenv(server_key), "client.id": socket.gethostname()}
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


def report_ack(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {str(msg)} with error {str(err)}")
    else:
        print(f"Message produced: {str(msg)}")


async def stream_data(table_name: str, producer):
    engine = create_engine(connection_string)
    conn = engine.connect()
    for row in conn.execute(text(f"SELECT * FROM {table_name}")):
        producer.produce(
            topic=topic, key=table_name, value=str(row), callback=report_ack
        )
        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method call if the message is acknowledged.
        producer.poll(2)
    conn.close()
    engine.dispose()


async def main():
    producer = Producer(conf)
    tasks = [asyncio.create_task(stream_data(table, producer)) for table in oltp_tables]
    await asyncio.gather(*tasks)


asyncio.run(main())
