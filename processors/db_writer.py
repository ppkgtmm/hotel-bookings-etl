from typing import Any, Dict
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text, Table, MetaData, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import create_engine, text, Table, MetaData, update
from sqlalchemy.dialects.mysql import insert
from processors.constants import *
from datetime import timedelta

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
        state=query.inserted.state, country=query.inserted.country
    )
    conn.execute(query)
    conn.commit()


def stage_rooms(rows: list[Dict[str, Any]]):
    query = insert(Room).values(rows)
    conn.execute(query)
    conn.commit()


def stage_guests(rows: list[Dict[str, Any]]):
    query = insert(Guest).values(rows)
    conn.execute(query)
    conn.commit()


def write_dim_guests(rows: list[Dict[str, Any]]):
    query = insert(DimGuest).values(rows)
    query = query.on_duplicate_key_update(
        email=query.inserted.email, dob=query.inserted.dob, gender=query.inserted.gender
    )
    conn.execute(query)
    conn.commit()


def stage_bookings(rows: list[Dict[str, Any]]):
    query = insert(Booking).values(rows)
    query = query.on_duplicate_key_update(
        checkin=query.inserted.checkin, checkout=query.inserted.checkout
    )
    conn.execute(query)
    conn.commit()


def stage_booking_rooms(rows: list[Dict[str, Any]]):
    query = insert(BookingRoom).values(rows)
    query = query.on_duplicate_key_update(
        booking=query.inserted.booking,
        room=query.inserted.room,
        guest=query.inserted.guest,
        updated_at=query.inserted.updated_at,
    )
    conn.execute(query)
    conn.commit()


def write_fct_bookings():
    query = bookings_query.format(
        stg_guest_table=stg_guest_table,
        stg_room_table=stg_room_table,
        stg_booking_table=stg_booking_table,
        stg_booking_room_table=stg_booking_room_table,
    )
    for row in conn.execute(text()):
        (
            id,
            checkin,
            checkout,
            guest,
            guest_location,
            room_type,
        ) = row
        current_date = checkin
        while current_date <= checkout:
            data = {
                "guest": guest,
                "guest_location": guest_location,
                "roomtype": room_type,
                "datetime": int(current_date.strftime("%Y%m%d%H%M%S")),
            }
            query = insert(FactBooking).values(**data)
            query = query.on_duplicate_key_update(**data)
            conn.execute(query)
            conn.commit()
            mark_processed = (
                update(BookingRoom).where(BookingRoom.c.id == id).values(processed=True)
            )
            conn.execute(mark_processed)
            conn.commit()
            current_date += timedelta(days=1)


def tear_down():
    conn.close()
    engine.dispose()
