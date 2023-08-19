from sqlalchemy import create_engine, Table, MetaData, NullPool, delete, select, func
from common import *
import pandas as pd

booking_id = 1
oltp_engine = create_engine(oltp_conn_str, poolclass=NullPool)
oltp_conn = oltp_engine.connect()
olap_engine = create_engine(olap_conn_str, poolclass=NullPool)
olap_conn = olap_engine.connect()
metadata = MetaData()
Booking = Table(bookings_table, metadata, autoload_with=oltp_engine)
BookingRoom = Table(booking_rooms_table, metadata, autoload_with=oltp_engine)
BookingAddon = Table(booking_addons_table, metadata, autoload_with=oltp_engine)
FactBooking = Table(fct_booking_table, metadata, autoload_with=olap_engine)
FactPurchase = Table(fct_purchase_table, metadata, autoload_with=olap_engine)
DimAddon = Table(dim_addon_table, metadata, autoload_with=olap_engine)

fp = open(delete_md, "w")

booking_q = select(Booking).where(Booking.c.id == booking_id)
booking = oltp_conn.execute(booking_q).fetchone()._asdict()

fp.write("booking :\n\n" + pd.DataFrame([booking]).to_markdown(index=False))

booking_room_q = select(BookingRoom).where(BookingRoom.c.booking == booking_id)
booking_rooms = [br._asdict() for br in oltp_conn.execute(booking_room_q).fetchall()]

fp.write(
    "\n\nbooking_rooms :\n\n" + pd.DataFrame(booking_rooms).to_markdown(index=False)
)

guests = {br["id"]: br["guest"] for br in booking_rooms}
booking_rooms = [br["id"] for br in booking_rooms]
booking_addon_q = select(BookingAddon).where(
    BookingAddon.c.booking_room.in_(booking_rooms)
)
booking_addons = [ba._asdict() for ba in oltp_conn.execute(booking_addon_q).fetchall()]

fp.write(
    "\n\nbooking_addons :\n\n" + pd.DataFrame(booking_addons).to_markdown(index=False)
)

for ba in booking_addons:
    datetime, guest = ba["datetime"], guests[ba["booking_room"]]
    addon_q = (
        select(func.max(DimAddon.c.id))
        .where(DimAddon.c.created_at <= ba["updated_at"])
        .where(DimAddon.c._id == ba["addon"])
    )
    addon = olap_conn.execute(addon_q).fetchone()[0]
    del_booking_addon_q = delete(BookingAddon).where(BookingAddon.c.id == ba["id"])
    oltp_conn.execute(del_booking_addon_q)
    oltp_conn.commit()
    # sleep(45)
    # fct_purchase_q = (
    #     select(FactPurchase)
    #     .where(FactPurchase.c.guest == guest)
    #     .where(FactPurchase.c.datetime == int(datetime.strftime(dt_fmt)))
    #     .where(FactPurchase.c.addon == addon)
    # )
    # fct_purchase = olap_conn.execute(fct_purchase_q).fetchall()
    # print("\nfct_purchase :\n\n" + pd.DataFrame(fct_purchase).to_markdown(index=False))
    # assert len(fct_purchase) == 0

del_booking_room_q = delete(BookingRoom).where(BookingRoom.c.booking == booking_id)
oltp_conn.execute(del_booking_room_q)
oltp_conn.commit()
del_booking_q = delete(Booking).where(Booking.c.id == booking_id)
oltp_conn.execute(del_booking_q)
oltp_conn.commit()

# fct_booking_q = (
#     select(FactBooking)
#     .where(FactBooking.c.guest in guests)
#     .where(FactBooking.c.datetime >= int(checkin.strftime(dt_fmt)))
#     .where(FactBooking.c.datetime <= int(checkout.strftime(dt_fmt)))
# )
# fct_booking = olap_conn.execute(fct_booking_q).fetchall()
# print("\nfct_booking :\n\n" + pd.DataFrame(fct_booking).to_markdown(index=False))

# assert len(fct_booking) == 0

fp.close()
oltp_conn.close()
oltp_engine.dispose()
olap_conn.close()
olap_engine.dispose()
