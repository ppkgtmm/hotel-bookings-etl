from dotenv import load_dotenv
from os import getenv, path
import pandas as pd

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

results_dir = path.join(path.abspath(path.dirname(__file__)), "results")
booking_file = path.join(results_dir, "booking.csv")
booking_room_file = path.join(results_dir, "booking_room.csv")
booking_addon_file = path.join(results_dir, "booking_addon.csv")


def get_connection_str():
    return {
        "oltp": f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{oltp_db}",
        "olap": f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{olap_db}",
    }


def get_facts(booking_file: str, booking_room_file: str, booking_addon_file: str):
    booking = pd.read_csv(booking_file)
    booking_rooms = pd.read_csv(booking_room_file)
    booking_addons = pd.read_csv(booking_addon_file)

    booking = booking[["id", "checkin", "checkout"]]
    booking_addons = booking_addons[
        ["id", "datetime", "addon", "booking_room", "quantity"]
    ]

    booking["checkin"] = pd.to_datetime(booking["checkin"])
    booking["checkout"] = pd.to_datetime(booking["checkout"])
    booking_addons["datetime"] = pd.to_datetime(booking_addons["datetime"])

    fct_booking = (
        booking.set_index("id")
        .join(booking_rooms.set_index("booking"))
        .rename(columns={"id": "booking_room_id"})
    )

    fct_amenities = (
        booking_rooms.set_index("id")
        .join(booking_addons.set_index("booking_room"))
        .dropna()
        .rename(columns={"id": "booking_addon_id"})
    )
    return fct_booking, fct_amenities
