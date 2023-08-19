from sqlalchemy import create_engine, Table, MetaData, NullPool, select
from common import *
import pandas as pd

olap_engine = create_engine(olap_conn_str, poolclass=NullPool)
olap_conn = olap_engine.connect()
metadata = MetaData()
FactBooking = Table(fct_booking_table, metadata, autoload_with=olap_engine)
FactPurchase = Table(fct_purchase_table, metadata, autoload_with=olap_engine)

booking = pd.read_csv(deleted_booking)
booking = booking[["id", "checkin", "checkout"]]
booking["checkin"] = pd.to_datetime(booking["checkin"])
booking["checkout"] = pd.to_datetime(booking["checkout"])
booking_rooms = pd.read_csv(deleted_booking_rooms)

fct_booking = booking.set_index("id").join(booking_rooms.set_index("booking"))
fct_booking = fct_booking[["id", "checkin", "checkout", "guest"]]
fct_booking = fct_booking.rename(columns={"id": "booking_room_id"})

for row in fct_booking.to_dict(orient="records"):
    query = (
        select(FactBooking)
        .where(FactBooking.c.guest == row["guest"])
        .where(FactBooking.c.datetime >= int(row["checkin"].strftime(dt_fmt)))
        .where(FactBooking.c.datetime <= int(row["checkout"].strftime(dt_fmt)))
    )
    assert olap_conn.execute(query).fetchall() == []
    query = (
        select(FactPurchase)
        .where(FactPurchase.c.guest == row["guest"])
        .where(FactPurchase.c.datetime >= int(row["checkin"].strftime(dt_fmt)))
        .where(FactPurchase.c.datetime <= int(row["checkout"].strftime(dt_fmt)))
    )
    assert olap_conn.execute(query).fetchall() == []

olap_conn.close()
olap_engine.dispose()
