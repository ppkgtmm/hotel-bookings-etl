from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()

DB_HOST_INTERNAL = os.getenv("DB_HOST_INTERNAL")
DBZ_USER = os.getenv("DBZ_USER")
DBZ_PASSWORD = os.getenv("DBZ_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
OLTP_DB = os.getenv("OLTP_DB")
DBZ_CONNECTOR = os.getenv("DBZ_CONNECTOR")
KAFKA_CONNECT_SERVER = os.getenv("KAFKA_CONNECT_SERVER")
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")

if __name__ == "__main__":
    config_path = f"{os.path.dirname(__file__)}/config.json"
    with open(config_path, "r") as fp:
        config = fp.read()
    config = (
        config.replace("${DBZ_CONNECTOR}", DBZ_CONNECTOR)
        .replace("${DB_HOST_INTERNAL}", DB_HOST_INTERNAL)
        .replace("${DBZ_USER}", DBZ_USER)
        .replace("${DBZ_PASSWORD}", DBZ_PASSWORD)
        .replace("${DB_PORT}", DB_PORT)
        .replace("${OLTP_DB}", OLTP_DB)
        .replace(
            "${KAFKA_BOOTSTRAP_SERVERS_INTERNAL}", KAFKA_BOOTSTRAP_SERVERS_INTERNAL
        )
    )
    response = requests.post(
        f"{KAFKA_CONNECT_SERVER}/connectors/",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=json.loads(config),
    )
    assert response.status_code == 201
