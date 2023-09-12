from typing import Any, Dict
from sqlalchemy import create_engine, text, Table, MetaData, update, delete
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.mysql import insert
from utilities.constants import *
from datetime import timedelta

dt_fmt = "%Y%m%d%H%M%S"
batch_size = 500


class DatabaseWriter:
    def __init__(
        self, db_user: str, db_password: str, db_host: str, db_port: str, db_name: str
    ):
        connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
            db_user, db_password, db_host, db_port, db_name
        )
        self.engine = create_engine(connection_string, poolclass=NullPool)

        self.metadata = MetaData()

        # staging tables
        self.Room = Table(stg_room_table, self.metadata, autoload_with=self.engine)
        self.Guest = Table(stg_guest_table, self.metadata, autoload_with=self.engine)
        self.Booking = Table(
            stg_booking_table, self.metadata, autoload_with=self.engine
        )
        self.DelBooking = Table(
            del_booking_table, self.metadata, autoload_with=self.engine
        )
        self.BookingRoom = Table(
            stg_booking_room_table, self.metadata, autoload_with=self.engine
        )
        self.DelBookingRoom = Table(
            del_booking_room_table, self.metadata, autoload_with=self.engine
        )
        self.BookingAddon = Table(
            stg_booking_addon_table, self.metadata, autoload_with=self.engine
        )
        self.DelBookingAddon = Table(
            del_booking_addon_table, self.metadata, autoload_with=self.engine
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
        with self.engine.connect() as conn:
            for i in range(0, len(rows), batch_size):
                query = insert(self.DimDate).values(rows[i : i + batch_size])
                query = query.on_duplicate_key_update(
                    datetime=query.inserted.datetime,
                    date=query.inserted.date,
                    month=query.inserted.month,
                    quarter=query.inserted.quarter,
                    year=query.inserted.year,
                )
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

    def del_bookings(self, rows: list[Dict[str, Any]]):
        query = insert(self.DelBooking).values(rows)
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
            processed=False,
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def del_booking_rooms(self, rows: list[Dict[str, Any]]):
        query = insert(self.DelBookingRoom).values(rows)
        query = query.on_duplicate_key_update(
            booking=query.inserted.booking,
            room=query.inserted.room,
            guest=query.inserted.guest,
            updated_at=query.inserted.updated_at,
            processed=False,
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
            processed=False,
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def del_booking_addons(self, rows: list[Dict[str, Any]]):
        query = insert(self.DelBookingAddon).values(rows)
        query = query.on_duplicate_key_update(
            booking_room=query.inserted.booking_room,
            addon=query.inserted.addon,
            quantity=query.inserted.quantity,
            datetime=query.inserted.datetime,
            updated_at=query.inserted.updated_at,
            processed=False,
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
            dim_roomtype_table=dim_roomtype_table,
            dim_location_table=dim_location_table,
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
                        datetime=int(current_date.strftime(dt_fmt)),
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

    def remove_fct_bookings(self):
        query = remove_bookings_query.format(
            del_booking_room_table=del_booking_room_table,
            del_booking_table=del_booking_table,
            stg_booking_table=stg_booking_table,
        )
        with self.engine.connect() as conn:
            for row in conn.execute(text(query)):
                (id, checkin, checkout, guest) = row
                query = (
                    delete(self.FactBooking)
                    .where(self.FactBooking.c.guest == guest)
                    .where(self.FactBooking.c.datetime >= int(checkin.strftime(dt_fmt)))
                    .where(
                        self.FactBooking.c.datetime <= int(checkout.strftime(dt_fmt))
                    )
                )
                conn.execute(query)
                conn.commit()

                mark_processed = (
                    update(self.DelBookingRoom)
                    .where(self.DelBookingRoom.c.id == id)
                    .values(processed=True)
                )
                conn.execute(mark_processed)
                conn.commit()

    def write_fct_purchases(self):
        query = purchases_query.format(
            stg_guest_table=stg_guest_table,
            stg_room_table=stg_room_table,
            stg_booking_addon_table=stg_booking_addon_table,
            stg_booking_room_table=stg_booking_room_table,
            dim_addon_table=dim_addon_table,
            dim_roomtype_table=dim_roomtype_table,
            dim_location_table=dim_location_table,
        )
        with self.engine.connect() as conn:
            for row in conn.execute(text(query)):
                (id, datetime, guest, guest_location, room_type, addon, quantity) = row
                query = insert(self.FactPurchase).values(
                    guest=guest,
                    guest_location=guest_location,
                    roomtype=room_type,
                    datetime=int(datetime.strftime(dt_fmt)),
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

    def remove_fct_purchases(self):
        query = remove_purchases_query.format(
            del_booking_addon_table=del_booking_addon_table,
            del_booking_room_table=del_booking_room_table,
            stg_booking_room_table=stg_booking_room_table,
            dim_addon_table=dim_addon_table,
        )
        with self.engine.connect() as conn:
            for row in conn.execute(text(query)):
                (id, datetime, guest, addon) = row
                query = (
                    delete(self.FactPurchase)
                    .where(self.FactPurchase.c.guest == guest)
                    .where(
                        self.FactPurchase.c.datetime == int(datetime.strftime(dt_fmt))
                    )
                    .where(self.FactPurchase.c.addon == addon)
                )
                conn.execute(query)
                conn.commit()

                mark_processed = (
                    update(self.DelBookingAddon)
                    .where(self.DelBookingAddon.c.id == id)
                    .values(processed=True)
                )
                conn.execute(mark_processed)
                conn.commit()

    def tear_down(self):
        self.engine.dispose()
