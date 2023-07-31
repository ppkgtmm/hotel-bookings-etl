import json
from time import sleep
from dotenv import load_dotenv
import os
import requests

load_dotenv()

DB_HOST = os.getenv("DB_HOST_INTERNAL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
OLTP_DB = os.getenv("OLTP_DB")
OLAP_DB = os.getenv("OLAP_DB")
KAFKA_CONNECT_SERVER = os.getenv("KAFKA_CONNECT_SERVER")
KAFKA_INTERNAL = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")
LOCATION_TABLE = os.getenv("LOCATION_TABLE")
GUESTS_TABLE = os.getenv("GUESTS_TABLE")
ADDONS_TABLE = os.getenv("ADDONS_TABLE")
ROOMTYPES_TABLE = os.getenv("ROOMTYPES_TABLE")
ROOMS_TABLE = os.getenv("ROOMS_TABLE")
BOOKINGS_TABLE = os.getenv("BOOKINGS_TABLE")
BOOKING_ROOMS_TABLE = os.getenv("BOOKING_ROOMS_TABLE")
BOOKING_ADDONS_TABLE = os.getenv("BOOKING_ADDONS_TABLE")

OLTP_TABLES = [
    LOCATION_TABLE,
    GUESTS_TABLE,
    ADDONS_TABLE,
    ROOMTYPES_TABLE,
    ROOMS_TABLE,
    BOOKINGS_TABLE,
    BOOKING_ROOMS_TABLE,
    BOOKING_ADDONS_TABLE,
]

config_path = f"{os.path.dirname(__file__)}/config.json"


def get_config(table_name, **kwargs):
    with open(config_path, "r") as fp:
        config = json.load(fp)
    config["name"] = kwargs.get("DB_NAME") + "." + table_name
    config["config"]["database.hostname"] = kwargs.get("DB_HOST")
    config["config"]["database.port"] = kwargs.get("DB_PORT")
    config["config"]["database.user"] = kwargs.get("DB_USER")
    config["config"]["database.password"] = kwargs.get("DB_PASSWORD")
    config["config"]["topic.prefix"] = kwargs.get("DB_NAME")
    config["config"]["database.include.list"] = kwargs.get("DB_NAME")
    config["config"]["table.include.list"] = kwargs.get("DB_NAME") + "." + table_name
    config["config"]["schema.history.internal.kafka.bootstrap.servers"] = KAFKA_INTERNAL
    config["config"][
        "schema.history.internal.kafka.topic"
    ] = f"schema-changes.{kwargs.get('DB_NAME')}"
    return config


if __name__ == "__main__":
    mysql_kwargs = dict(
        DB_HOST=DB_HOST, DB_USER=DB_USER, DB_PASSWORD=DB_PASSWORD, DB_PORT=DB_PORT
    )
    for table_name in OLTP_TABLES:
        config = get_config(table_name, **dict(**mysql_kwargs, DB_NAME=OLTP_DB))
        response = requests.post(
            f"{KAFKA_CONNECT_SERVER}/connectors/",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=config,
        )
        assert response.status_code == 201
        sleep(2)
