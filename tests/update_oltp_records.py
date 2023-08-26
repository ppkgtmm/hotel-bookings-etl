from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    NullPool,
    delete,
    select,
    update,
    and_,
    or_,
    not_,
)
from common import *
import pandas as pd
from datetime import datetime, timedelta

booking_id = 2
checkin = datetime(year=2023, month=1, day=15)
checkout = checkin + timedelta(days=5)

oltp_engine = create_engine(oltp_conn_str, poolclass=NullPool)
oltp_conn = oltp_engine.connect()
metadata = MetaData()
Booking = Table(bookings_table, metadata, autoload_with=oltp_engine)
BookingRoom = Table(booking_rooms_table, metadata, autoload_with=oltp_engine)
BookingAddon = Table(booking_addons_table, metadata, autoload_with=oltp_engine)


booking_q = select(Booking).where(Booking.c.id == booking_id)
booking = oltp_conn.execute(booking_q).fetchone()._asdict()

print("writing booking before update")
pd.DataFrame([booking]).to_csv(booking_before, index=False)

booking_room_q = select(BookingRoom).where(BookingRoom.c.booking == booking_id)
booking_rooms = [br._asdict() for br in oltp_conn.execute(booking_room_q).fetchall()]

print("writing booking rooms before update")
pd.DataFrame(booking_rooms).to_csv(booking_room_before, index=False)
guests = [br["guest"] for br in booking_rooms]
booking_rooms = [br["id"] for br in booking_rooms]

update_booking_q = (
    update(Booking)
    .where(Booking.c.id == booking_id)
    .values(checkin=checkin, checkout=checkout)
)
oltp_conn.execute(update_booking_q)
oltp_conn.commit()

booking["checkin"] = checkin
booking["checkout"] = checkout
pd.DataFrame([booking]).to_csv(booking_after, index=False)

booking_room_q = (
    select(BookingRoom)
    .outerjoin(Booking, Booking.c.id == BookingRoom.c.booking)
    .where(
        not_(
            or_(
                and_(Booking.c.checkin <= checkin, checkin <= Booking.c.checkout),
                and_(Booking.c.checkin <= checkout, checkout <= Booking.c.checkout),
                and_(checkin <= Booking.c.checkin, checkout >= Booking.c.checkout),
            )
        )
    )
    .where(not_(BookingRoom.c.guest.in_(guests)))
)
updated_booking_rooms = []
for i, br in enumerate(oltp_conn.execute(booking_room_q).fetchmany(len(booking_rooms))):
    room = br._asdict()["room"]
    id = booking_rooms[i]
    update_booking_room_q = (
        update(BookingRoom).where(BookingRoom.c.id == id).values(room=room)
    )
    oltp_conn.execute(update_booking_room_q)
    oltp_conn.commit()
    updated_booking_room = oltp_conn.execute(
        select(BookingRoom).where(BookingRoom.c.id == id)
    ).fetchone()
    updated_booking_rooms.append(updated_booking_room._asdict())

pd.DataFrame(updated_booking_rooms).to_csv(booking_room_after, index=False)

oltp_conn.close()
oltp_engine.dispose()
