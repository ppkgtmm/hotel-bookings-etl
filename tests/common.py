from dotenv import load_dotenv
from os import getenv
import sys
from os.path import abspath, dirname, join
import pandas as pd

sys.path.append(dirname(dirname(abspath(__file__))))
from utilities.constants import (
    fct_booking_table,
    fct_purchase_table,
    dim_addon_table,
)

dt_fmt = "%Y%m%d%H%M%S"
result_folder = join(dirname(abspath(__file__)), "results")
booking_before = join(result_folder, "booking_before.csv")
booking_after = join(result_folder, "booking_after.csv")
booking_room_before = join(result_folder, "booking_room_before.csv")
booking_room_after = join(result_folder, "booking_room_after.csv")
booking_addon_before = join(result_folder, "booking_addon_before.csv")
booking_addon_after = join(result_folder, "booking_addon_after.csv")
deleted_booking = join(result_folder, "deleted_booking.csv")
deleted_booking_rooms = join(result_folder, "deleted_booking_rooms.csv")
deleted_booking_addons = join(result_folder, "deleted_booking_addons.csv")

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
oltp_db = getenv("OLTP_DB")
olap_db = getenv("OLAP_DB")
location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
booking_addons_table = getenv("BOOKING_ADDONS_TABLE")

conn_str = "mysql+mysqlconnector://{}:{}@{}:{}/{}"
oltp_conn_str = conn_str.format(db_user, db_password, db_host, db_port, oltp_db)
olap_conn_str = conn_str.format(db_user, db_password, db_host, db_port, olap_db)


def get_facts(booking_file, booking_room_file, booking_addon_file):
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

    fct_purchase = (
        booking_rooms.set_index("id")
        .join(booking_addons.set_index("booking_room"))
        .dropna()
        .rename(columns={"id": "booking_addon_id"})
    )
    return fct_booking, fct_purchase
