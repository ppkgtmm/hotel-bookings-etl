from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from os import getenv


load_dotenv()


server_key = "KAFKA_BOOTSTRAP_SERVERS"


conf = {
    "bootstrap.servers": getenv(server_key),
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

if __name__ == "__main__":
    admin = AdminClient(conf)
    for table in oltp_tables:
        admin.create_topics([NewTopic(table)])[table].result()
