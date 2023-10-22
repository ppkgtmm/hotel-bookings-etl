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
    conn_str_map = get_connection_str()
    olap_engine = create_engine(conn_str_map.get("olap"))
    olap_conn = olap_engine.connect()
    fct_booking = Table(fct_booking_table, MetaData(), autoload_with=olap_engine)
    dim_guest = Table(dim_guest_table, MetaData(), autoload_with=olap_engine)
    dim_addon = Table(dim_addon_table, MetaData(), autoload_with=olap_engine)
    fct_amenities = Table(fct_amenities_table, MetaData(), autoload_with=olap_engine)

    @classmethod
    def get_dim_guest(cls, _id: int):
        guest_query = (
            select(cls.dim_guest.c.id)
            .where(cls.dim_guest.c._id == _id)
            .order_by(desc(cls.dim_guest.c.id))
            .limit(1)
        )
        return cls.olap_conn.execute(guest_query).fetchone()

    @classmethod
    def get_dim_addon(cls, _id: int):
        addon_query = (
            select(cls.dim_addon.c.id)
            .where(cls.dim_addon.c._id == _id)
            .order_by(desc(cls.dim_addon.c.id))
            .limit(1)
        )
        return cls.olap_conn.execute(addon_query).fetchone()

    @classmethod
    def get_fct_bookings(cls, booking: dict, booking_room: dict):
        checkin = datetime.fromisoformat(booking["checkin"]).strftime(fmt)
        checkout = datetime.fromisoformat(booking["checkout"]).strftime(fmt)
        guest = cls.get_dim_guest(booking_room["guest"])
        if not guest:
            return []
        booking_query = (
            select(cls.fct_booking)
            .where(cls.fct_booking.c.datetime >= int(checkin))
            .where(cls.fct_booking.c.datetime <= int(checkout))
            .where(cls.fct_booking.c.guest == guest[0])
        )
        return cls.olap_conn.execute(booking_query).fetchall()

    @classmethod
    def get_fct_amenities(cls, booking_room: dict, booking_addon: dict):
        date_time = datetime.fromisoformat(booking_addon["datetime"]).strftime(fmt)
        guest = cls.get_dim_guest(booking_room["guest"])
        addon = cls.get_dim_addon(booking_addon["addon"])
        if None in [guest, addon]:
            return []
        amenities_query = (
            select(cls.fct_amenities)
            .where(cls.fct_amenities.c.datetime == int(date_time))
            .where(cls.fct_amenities.c.addon <= addon[0])
            .where(cls.fct_amenities.c.guest == guest[0])
        )
        return cls.olap_conn.execute(amenities_query).fetchall()

    @classmethod
    def del_fct_bookings(cls, fct_bookings: list):
        ids = [row._asdict()["id"] for row in fct_bookings]
        booking_query = delete(cls.fct_booking).where(cls.fct_booking.c.id.in_(ids))
        cls.olap_conn.execute(booking_query)
        cls.olap_conn.commit()

    @classmethod
    def del_fct_amenities(cls, fct_amenities: list):
        ids = [row._asdict()["id"] for row in fct_amenities]
        amenities_query = delete(cls.fct_amenities).where(
            cls.fct_amenities.c.id.in_(ids)
        )
        cls.olap_conn.execute(amenities_query)
        cls.olap_conn.commit()

    @classmethod
    def tear_down(cls):
        cls.olap_conn.close()
        cls.olap_engine.dispose()
