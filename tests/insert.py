from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData, select, insert
from common import get_connection_str, get_insert_query

load_dotenv()

# location_table = getenv("LOCATION_TABLE")
# addons_table = getenv("ADDONS_TABLE")
# roomtypes_table = getenv("ROOMTYPES_TABLE")
guests_table = getenv("GUESTS_TABLE")
rooms_table = getenv("ROOMS_TABLE")
users_table = getenv("USERS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
# booking_addons_table = getenv("BOOKING_ADDONS_TABLE")

if __name__ == "__main__":
    conn_str_map = get_connection_str()
    oltp_engine = create_engine(conn_str_map.get("oltp"))
    olap_engine = create_engine(conn_str_map.get("olap"))

    oltp_engine.dispose()
    olap_engine.dispose()
