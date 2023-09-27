from dotenv import load_dotenv
from os import getenv, path
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select, desc
from datetime import datetime


load_dotenv()

db_host = getenv("DB_HOST")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_port = getenv("DB_PORT")
oltp_db = getenv("OLTP_DB")
olap_db = getenv("OLAP_DB")
fmt = getenv("DT_FORMAT")

dim_guest_table = getenv("DIM_GUEST_TABLE")
dim_addon_table = getenv("DIM_ADDON_TABLE")
fct_booking_table = getenv("FCT_BOOKING_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")

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


class TestHelper:
    def __init__(self):
        conn_str_map = get_connection_str()
        self.olap_engine = create_engine(conn_str_map.get("olap"))
        self.olap_conn = self.olap_engine.connect()
        self.fct_booking = Table(
            fct_booking_table, MetaData(), autoload_with=self.olap_engine
        )
        self.dim_guest = Table(
            dim_guest_table, MetaData(), autoload_with=self.olap_engine
        )
        self.dim_addon = Table(
            dim_addon_table, MetaData(), autoload_with=self.olap_engine
        )

    def get_fct_bookings(self, booking: dict, booking_room: dict):
        checkin = datetime.fromisoformat(booking["checkin"]).strftime(fmt)
        checkout = datetime.fromisoformat(booking["checkout"]).strftime(fmt)
        guest_query = (
            select(self.dim_guest.c.id)
            .where(self.dim_guest.c._id == booking_room["guest"])
            .order_by(desc(self.dim_guest.c.id))
            .limit(1)
        )
        guest = self.olap_conn.execute(guest_query).fetchone()[0]
        booking_query = (
            select(self.fct_booking)
            .where(self.fct_booking.c.datetime >= int(checkin))
            .where(self.fct_booking.c.datetime <= int(checkout))
            .where(self.fct_booking.c.guest == guest)
        )
        return self.olap_conn.execute(booking_query).fetchall()

    # def get_fct_amenities(self, booking_room: dict, booking_addon: dict):
    #     guest_query = (
    #         select(self.dim_guest.c.id)
    #         .where(self.dim_guest.c._id == booking_room["guest"])
    #         .order_by(desc(self.dim_guest.c.id))
    #         .limit(1)
    #     )
    #     guest = self.olap_conn.execute(guest_query).fetchone()[0]
    #     addon_query = (
    #         select(self.dim_addon.c.id)
    #         .where(self.dim_guest.c._id == booking_room["guest"])
    #         .order_by(desc(self.dim_guest.c.id))
    #         .limit(1)
    #     )
    #     guest = self.olap_conn.execute(guest_query).fetchone()[0]

    #     booking_query = (
    #         select(self.fct_booking)
    #         .where(self.fct_booking.c.datetime >= checkin)
    #         .where(self.fct_booking.c.datetime <= checkout)
    #         .where(self.fct_booking.c.guest == guest)
    #     )
    #     return self.olap_conn.execute(booking_query).fetchall()

    def tear_down(self):
        self.olap_conn.close()
        self.olap_engine.dispose()
