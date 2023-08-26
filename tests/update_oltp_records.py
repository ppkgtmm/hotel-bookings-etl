from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    NullPool,
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
booking_rooms = pd.DataFrame(booking_rooms)

print("writing booking rooms before update")
booking_rooms.to_csv(booking_room_before, index=False)

update_booking_q = (
    update(Booking)
    .where(Booking.c.id == booking_id)
    .values(checkin=checkin, checkout=checkout)
)
oltp_conn.execute(update_booking_q)
oltp_conn.commit()

booking["checkin"], booking["checkout"] = checkin, checkout
print("writing booking after update")
pd.DataFrame([booking]).to_csv(booking_after, index=False)

avail_room_q = (
    select(BookingRoom.c.room)
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
    .where(not_(BookingRoom.c.guest.in_(booking_rooms.guest.tolist())))
)

avail_rooms = oltp_conn.execute(avail_room_q).fetchmany(booking_rooms.shape[0])
for i, br in enumerate(avail_rooms):
    room, id = br._asdict()["room"], int(booking_rooms.loc[i, "id"])
    update_booking_room_q = (
        update(BookingRoom).where(BookingRoom.c.id == id).values(room=room)
    )
    oltp_conn.execute(update_booking_room_q)
    oltp_conn.commit()
    booking_rooms.loc[i, "room"] = room

print("writing booking rooms after update")
pd.DataFrame(booking_rooms).to_csv(booking_room_after, index=False)


booking_addon = (
    oltp_conn.execute(
        select(BookingAddon).where(
            BookingAddon.c.booking_room == int(booking_rooms.loc[0, "id"])
        )
    )
    .fetchone()
    ._asdict()
)

print("writing booking addon before update")
pd.DataFrame([booking_addon]).to_csv(booking_addon_before, index=False)

booking_addon["quantity"] = booking_addon["quantity"] + 1
oltp_conn.execute(
    update(BookingAddon)
    .where(BookingAddon.c.id == booking_addon["id"])
    .values(quantity=booking_addon["quantity"])
)
oltp_conn.commit()

print("writing booking addon after update")
pd.DataFrame([booking_addon]).to_csv(booking_addon_after, index=False)

oltp_conn.close()
oltp_engine.dispose()
