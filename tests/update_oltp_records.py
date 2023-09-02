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

updated_booking = {**booking, "checkin": checkin, "checkout": checkout}
print("writing booking after update")
pd.DataFrame([updated_booking]).to_csv(booking_after, index=False)

avail_guest_q = (
    select(BookingRoom.c.guest)
    .outerjoin(Booking, Booking.c.id == BookingRoom.c.booking)
    .where(
        not_(
            or_(
                and_(Booking.c.checkin <= checkin, checkin <= Booking.c.checkout),
                and_(Booking.c.checkin <= checkout, checkout <= Booking.c.checkout),
                and_(checkin <= Booking.c.checkin, checkout >= Booking.c.checkout),
                and_(
                    Booking.c.checkin <= booking["checkin"],
                    booking["checkin"] <= Booking.c.checkout,
                ),
                and_(
                    Booking.c.checkin <= booking["checkout"],
                    booking["checkout"] <= Booking.c.checkout,
                ),
                and_(
                    booking["checkin"] <= Booking.c.checkin,
                    booking["checkout"] >= Booking.c.checkout,
                ),
            )
        )
    )
    .where(not_(BookingRoom.c.guest.in_(booking_rooms.guest.tolist())))
)

avail_guest = oltp_conn.execute(avail_guest_q).fetchmany(booking_rooms.shape[0])
for i, br in enumerate(avail_guest):
    guest, id = br._asdict()["guest"], int(booking_rooms.loc[i, "id"])
    update_booking_room_q = (
        update(BookingRoom).where(BookingRoom.c.id == id).values(guest=guest)
    )
    oltp_conn.execute(update_booking_room_q)
    oltp_conn.commit()
    booking_rooms.loc[i, "guest"] = guest

print("writing booking rooms after update")
pd.DataFrame(booking_rooms).to_csv(booking_room_after, index=False)


booking_addon = (
    oltp_conn.execute(
        select(BookingAddon).where(
            BookingAddon.c.booking_room > int(booking_rooms["id"].max())
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
