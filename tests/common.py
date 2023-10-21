from dotenv import load_dotenv
from os import getenv, path
from sqlalchemy import create_engine, Table, MetaData, select, desc, delete
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
fct_amenities_table = getenv("FCT_AMENITIES_TABLE")

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
        self.fct_amenities = Table(
            fct_amenities_table, MetaData(), autoload_with=self.olap_engine
        )

    def get_dim_guest(self, _id: int):
        guest_query = (
            select(self.dim_guest.c.id)
            .where(self.dim_guest.c._id == _id)
            .order_by(desc(self.dim_guest.c.id))
            .limit(1)
        )
        return self.olap_conn.execute(guest_query).fetchone()

    def get_dim_addon(self, _id: int):
        addon_query = (
            select(self.dim_addon.c.id)
            .where(self.dim_addon.c._id == _id)
            .order_by(desc(self.dim_addon.c.id))
            .limit(1)
        )
        return self.olap_conn.execute(addon_query).fetchone()

    def get_fct_bookings(self, booking: dict, booking_room: dict):
        checkin = datetime.fromisoformat(booking["checkin"]).strftime(fmt)
        checkout = datetime.fromisoformat(booking["checkout"]).strftime(fmt)
        guest = self.get_dim_guest(booking_room["guest"])
        if not guest:
            return []
        booking_query = (
            select(self.fct_booking)
            .where(self.fct_booking.c.datetime >= int(checkin))
            .where(self.fct_booking.c.datetime <= int(checkout))
            .where(self.fct_booking.c.guest == guest[0])
        )
        return self.olap_conn.execute(booking_query).fetchall()

    def get_fct_amenities(self, booking_room: dict, booking_addon: dict):
        date_time = datetime.fromisoformat(booking_addon["datetime"]).strftime(fmt)
        guest = self.get_dim_guest(booking_room["guest"])
        addon = self.get_dim_addon(booking_addon["addon"])
        if None in [guest, addon]:
            return []
        amenities_query = (
            select(self.fct_amenities)
            .where(self.fct_amenities.c.datetime == int(date_time))
            .where(self.fct_amenities.c.addon <= addon[0])
            .where(self.fct_amenities.c.guest == guest[0])
        )
        return self.olap_conn.execute(amenities_query).fetchall()

    def del_fct_bookings(self, fct_bookings: list):
        ids = [row._asdict()["id"] for row in fct_bookings]
        booking_query = delete(self.fct_booking).where(self.fct_booking.c.id.in_(ids))
        self.olap_conn.execute(booking_query)

    def del_fct_amenities(self, fct_amenities: list):
        ids = [row._asdict()["id"] for row in fct_amenities]
        amenities_query = delete(self.fct_amenities).where(
            self.fct_amenities.c.id.in_(ids)
        )
        self.olap_conn.execute(amenities_query)

    def tear_down(self):
        self.olap_conn.close()
        self.olap_engine.dispose()
