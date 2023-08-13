import json
from dotenv import load_dotenv
import os
import requests

load_dotenv()

DB_HOST = os.getenv("DB_HOST_INTERNAL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("OLTP_DB")
KAFKA_CONNECT_SERVER = os.getenv("KAFKA_CONNECT_SERVER")
KAFKA_INTERNAL = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")

config_path = f"{os.path.dirname(__file__)}/config.json"


def get_config(**kwargs):
    with open(config_path, "r") as fp:
        config = json.load(fp)
    config["name"] = DB_NAME
    config["config"]["database.hostname"] = DB_HOST
    config["config"]["database.port"] = DB_PORT
    config["config"]["database.user"] = DB_USER
    config["config"]["database.password"] = DB_PASSWORD
    config["config"]["topic.prefix"] = DB_NAME
    config["config"]["database.include.list"] = DB_NAME
    config["config"]["schema.history.internal.kafka.bootstrap.servers"] = KAFKA_INTERNAL
    config["config"]["schema.history.internal.kafka.topic"] = "schema-changes" + DB_NAME
    return config


if __name__ == "__main__":
    config = get_config()
    response = requests.post(
        f"{KAFKA_CONNECT_SERVER}/connectors/",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=config,
    )
    assert response.status_code == 201
