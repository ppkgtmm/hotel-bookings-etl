from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from common import *
import pandas as pd

booking_id = 1
booking_q = f"SELECT * FROM {bookings_table} WHERE id = {booking_id}"
booking_room_q = f"SELECT * FROM {booking_rooms_table} WHERE booking = {booking_id}"
del_booking_q = f"DELETE FROM {bookings_table} WHERE id = {booking_id}"
del_booking_room_q = f"DELETE FROM {booking_rooms_table} WHERE booking = {booking_id}"

oltp_engine = create_engine(oltp_conn_str, poolclass=NullPool)
oltp_conn = oltp_engine.connect()

booking = dict(oltp_conn.execute(booking_q).fetchone())
print("writing booking to be deleted")
pd.DataFrame([booking]).to_csv(deleted_booking, index=False)

booking_rooms = [dict(r) for r in oltp_conn.execute(booking_room_q).fetchall()]
print("writing booking rooms to be deleted")
pd.DataFrame(booking_rooms).to_csv(deleted_booking_rooms, index=False)

booking_addons = []
for booking_room in booking_rooms:
    id = booking_room["id"]
    booking_addon_q = f"SELECT * FROM {booking_addons_table} WHERE booking_room = {id}"
    booking_addons += [dict(r) for r in oltp_conn.execute(booking_addon_q).fetchall()]
    del_booking_addon_q = (
        f"DELETE FROM {booking_addons_table} WHERE booking_room = {id}"
    )
    oltp_conn.execute(del_booking_addon_q)

print("writing booking addons to be deleted")
pd.DataFrame(booking_addons).to_csv(deleted_booking_addons, index=False)

oltp_conn.execute(del_booking_room_q)
oltp_conn.execute(del_booking_q)

oltp_conn.close()
oltp_engine.dispose()
