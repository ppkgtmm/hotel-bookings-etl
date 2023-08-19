from sqlalchemy import create_engine, Table, MetaData, NullPool, delete, select, func
from common import *
import pandas as pd

booking_id = 1
oltp_engine = create_engine(oltp_conn_str, poolclass=NullPool)
oltp_conn = oltp_engine.connect()
metadata = MetaData()
Booking = Table(bookings_table, metadata, autoload_with=oltp_engine)
BookingRoom = Table(booking_rooms_table, metadata, autoload_with=oltp_engine)
BookingAddon = Table(booking_addons_table, metadata, autoload_with=oltp_engine)


booking_q = select(Booking).where(Booking.c.id == booking_id)
booking = oltp_conn.execute(booking_q).fetchone()._asdict()

print("writing booking to be deleted")
pd.DataFrame([booking]).to_csv(deleted_booking, index=False)

booking_room_q = select(BookingRoom).where(BookingRoom.c.booking == booking_id)
booking_rooms = [br._asdict() for br in oltp_conn.execute(booking_room_q).fetchall()]

print("writing booking rooms to be deleted")
pd.DataFrame(booking_rooms).to_csv(deleted_booking_rooms, index=False)

booking_rooms = [br["id"] for br in booking_rooms]
booking_addon_q = select(BookingAddon).where(
    BookingAddon.c.booking_room.in_(booking_rooms)
)
booking_addons = [ba._asdict() for ba in oltp_conn.execute(booking_addon_q).fetchall()]

print("writing booking addons to be deleted")
pd.DataFrame(booking_addons).to_csv(deleted_booking_addons, index=False)

del_booking_addon_q = delete(BookingAddon).where(
    BookingAddon.c.booking_room.in_(booking_rooms)
)
oltp_conn.execute(del_booking_addon_q)
oltp_conn.commit()

del_booking_room_q = delete(BookingRoom).where(BookingRoom.c.booking == booking_id)
oltp_conn.execute(del_booking_room_q)
oltp_conn.commit()

del_booking_q = delete(Booking).where(Booking.c.id == booking_id)
oltp_conn.execute(del_booking_q)
oltp_conn.commit()

oltp_conn.close()
oltp_engine.dispose()
