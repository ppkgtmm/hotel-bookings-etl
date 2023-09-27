from typing import List
from dotenv import load_dotenv
from os import getenv

load_dotenv()

db_host = getenv("DB_HOST")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_port = getenv("DB_PORT")
oltp_db = getenv("OLTP_DB")
olap_db = getenv("OLAP_DB")

booking_id = 100000001
booking_room_ids = [100000001, 100000002]
booking_addon_ids = [100000001, 100000002]


def get_connection_str():
    return {
        "oltp": f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{oltp_db}",
        "olap": f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{olap_db}",
    }
