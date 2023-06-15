import json
from dotenv import load_dotenv
import os
import requests

load_dotenv()

db_password_key = "DB_PASSWORD"
connect_server_key = "KAFKA_CONNECT_SERVER"

config_path = f"{os.path.dirname(__file__)}/register_mysql.json"

if __name__ == "__main__":
    with open(config_path, "r") as fp:
        config = json.load(fp)
        config["config"]["database.password"] = os.getenv(db_password_key)
    response = requests.post(
        f"{os.getenv(connect_server_key)}/connectors/",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=config,
    )
    print(response.json())
