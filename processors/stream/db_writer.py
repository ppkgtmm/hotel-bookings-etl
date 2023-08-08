import sys
from os.path import dirname, abspath

sys.path.append(dirname(dirname(abspath(__file__))))
from typing import Any, Dict
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text, Table, MetaData, update, NullPool
from sqlalchemy.dialects.mysql import insert
from constants import *
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


class DatabaseWriter:
    def __init__(self) -> None:
        self.engine = create_engine(connection_string, poolclass=NullPool)

        self.metadata = MetaData()

        # staging tables
        self.Room = Table(stg_room_table, self.metadata, autoload_with=self.engine)
        self.Guest = Table(stg_guest_table, self.metadata, autoload_with=self.engine)
        self.Booking = Table(
            stg_booking_table, self.metadata, autoload_with=self.engine
        )
        self.BookingRoom = Table(
            stg_booking_room_table, self.metadata, autoload_with=self.engine
        )
        self.BookingAddon = Table(
            stg_booking_addon_table, self.metadata, autoload_with=self.engine
        )

        self.DimAddon = Table(dim_addon_table, self.metadata, autoload_with=self.engine)
        self.DimRoomType = Table(
            dim_roomtype_table, self.metadata, autoload_with=self.engine
        )
        self.DimDate = Table(dim_date_table, self.metadata, autoload_with=self.engine)
        self.DimGuest = Table(dim_guest_table, self.metadata, autoload_with=self.engine)
        self.DimLocation = Table(
            dim_location_table, self.metadata, autoload_with=self.engine
        )
        self.FactBooking = Table(
            fct_booking_table, self.metadata, autoload_with=self.engine
        )
        self.FactPurchase = Table(
            fct_purchase_table, self.metadata, autoload_with=self.engine
        )

    def write_dim_date(self, rows: list[Dict[str, Any]]):
        query = insert(self.DimDate).values(rows)
        query = query.on_duplicate_key_update(
            datetime=query.inserted.datetime,
            date=query.inserted.date,
            month=query.inserted.month,
            quarter=query.inserted.quarter,
            year=query.inserted.year,
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def write_dim_addons(self, rows: list[Dict[str, Any]]):
        query = insert(self.DimAddon).values(rows)
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def write_dim_roomtypes(self, rows: list[Dict[str, Any]]):
        query = insert(self.DimRoomType).values(rows)
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def write_dim_locations(self, rows: list[Dict[str, Any]]):
        query = insert(self.DimLocation).values(rows)
        query = query.on_duplicate_key_update(
            state=query.inserted.state, country=query.inserted.country
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def stage_rooms(self, rows: list[Dict[str, Any]]):
        query = insert(self.Room).values(rows)
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def stage_guests(self, rows: list[Dict[str, Any]]):
        query = insert(self.Guest).values(rows)
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def write_dim_guests(self, rows: list[Dict[str, Any]]):
        query = insert(self.DimGuest).values(rows)
        query = query.on_duplicate_key_update(
            email=query.inserted.email,
            dob=query.inserted.dob,
            gender=query.inserted.gender,
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def stage_bookings(self, rows: list[Dict[str, Any]]):
        query = insert(self.Booking).values(rows)
        query = query.on_duplicate_key_update(
            checkin=query.inserted.checkin, checkout=query.inserted.checkout
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def stage_booking_rooms(self, rows: list[Dict[str, Any]]):
        query = insert(self.BookingRoom).values(rows)
        query = query.on_duplicate_key_update(
            booking=query.inserted.booking,
            room=query.inserted.room,
            guest=query.inserted.guest,
            updated_at=query.inserted.updated_at,
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def stage_booking_addons(self, rows: list[Dict[str, Any]]):
        query = insert(self.BookingAddon).values(rows)
        query = query.on_duplicate_key_update(
            booking_room=query.inserted.booking_room,
            addon=query.inserted.addon,
            quantity=query.inserted.quantity,
            datetime=query.inserted.datetime,
            updated_at=query.inserted.updated_at,
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def write_fct_bookings(self):
        query = bookings_query.format(
            stg_guest_table=stg_guest_table,
            stg_room_table=stg_room_table,
            stg_booking_table=stg_booking_table,
            stg_booking_room_table=stg_booking_room_table,
        )
        with self.engine.connect() as conn:
            for row in conn.execute(text(query)):
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
                    query = insert(self.FactBooking).values(
                        guest=guest,
                        guest_location=guest_location,
                        roomtype=room_type,
                        datetime=int(current_date.strftime("%Y%m%d%H%M%S")),
                    )
                    query = query.on_duplicate_key_update(
                        datetime=query.inserted.datetime
                    )
                    conn.execute(query)
                    conn.commit()
                    mark_processed = (
                        update(self.BookingRoom)
                        .where(self.BookingRoom.c.id == id)
                        .values(processed=True)
                    )
                    conn.execute(mark_processed)
                    conn.commit()
                    current_date += timedelta(days=1)

    def write_fct_purchases(self):
        query = purchases_query.format(
            stg_guest_table=stg_guest_table,
            stg_room_table=stg_room_table,
            stg_booking_addon_table=stg_booking_addon_table,
            stg_booking_room_table=stg_booking_room_table,
        )
        with self.engine.connect() as conn:
            for row in conn.execute(text(query)):
                (id, datetime, guest, guest_location, room_type, addon, quantity) = row
                query = insert(self.FactPurchase).values(
                    guest=guest,
                    guest_location=guest_location,
                    roomtype=room_type,
                    datetime=int(datetime.strftime("%Y%m%d%H%M%S")),
                    addon=addon,
                    addon_quantity=quantity,
                )
                query = query.on_duplicate_key_update(
                    addon_quantity=query.inserted.addon_quantity
                )
                conn.execute(query)
                conn.commit()
                mark_processed = (
                    update(self.BookingAddon)
                    .where(self.BookingAddon.c.id == id)
                    .values(processed=True)
                )
                conn.execute(mark_processed)
                conn.commit()

    def tear_down(self):
        self.engine.dispose()
