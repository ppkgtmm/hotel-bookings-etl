from typing import Any, Dict
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text, Table, MetaData, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import create_engine, text, Table, MetaData, update
from sqlalchemy.dialects.mysql import insert
from processors.constants import *

load_dotenv()
db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")

connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    db_user, db_password, db_host, db_port, db_name
)
engine = create_engine(connection_string)
conn = engine.connect()

metadata = MetaData()

# staging tables
Location = Table(stg_location_table, metadata, autoload_with=engine)
Room = Table(stg_room_table, metadata, autoload_with=engine)
Guest = Table(stg_guest_table, metadata, autoload_with=engine)
Booking = Table(stg_booking_table, metadata, autoload_with=engine)
BookingRoom = Table(stg_booking_room_table, metadata, autoload_with=engine)
BookingAddon = Table(stg_booking_addon_table, metadata, autoload_with=engine)


DimAddon = Table(dim_addon_table, metadata, autoload_with=engine)
DimRoomType = Table(dim_roomtype_table, metadata, autoload_with=engine)
DimDate = Table(dim_date_table, metadata, autoload_with=engine)
DimGuest = Table(dim_guest_table, metadata, autoload_with=engine)
DimLocation = Table(dim_location_table, metadata, autoload_with=engine)
FactBooking = Table(fct_booking_table, metadata, autoload_with=engine)
FactPurchase = Table(fct_purchase_table, metadata, autoload_with=engine)


def write_dim_addons(rows: list[Dict[str, Any]]):
    query = insert(DimAddon).values(rows)
    conn.execute(query)
    conn.commit()


def write_dim_roomtypes(rows: list[Dict[str, Any]]):
    query = insert(DimRoomType).values(rows)
    conn.execute(query)
    conn.commit()


def write_dim_locations(rows: list[Dict[str, Any]]):
    query = insert(DimLocation).values(rows)
    query = query.on_duplicate_key_update(
        state=query.inserted.state,
        country=query.inserted.country,
    )
    conn.execute(query)
    conn.commit()
