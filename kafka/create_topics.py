from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from os import getenv


load_dotenv()
broker = getenv("KAFKA_BOOTSTRAP_SERVERS")
required_topics = [
    getenv("LOCATION_TABLE"),
    getenv("GUESTS_TABLE"),
    getenv("ADDONS_TABLE"),
    getenv("ROOMTYPES_TABLE"),
    getenv("ROOMS_TABLE"),
    getenv("BOOKINGS_TABLE"),
    getenv("BOOKING_ROOMS_TABLE"),
    getenv("BOOKING_ADDONS_TABLE"),
]

if __name__ == "__main__":
    admin = AdminClient(conf={"bootstrap.servers": broker})
    for topic in required_topics:
        admin.create_topics([NewTopic(topic)])[topic].result()
