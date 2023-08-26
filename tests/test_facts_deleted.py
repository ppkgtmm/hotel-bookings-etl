from sqlalchemy import create_engine, Table, MetaData, NullPool, select
from common import *

olap_engine = create_engine(olap_conn_str, poolclass=NullPool)
olap_conn = olap_engine.connect()
metadata = MetaData()
FactBooking = Table(fct_booking_table, metadata, autoload_with=olap_engine)
FactPurchase = Table(fct_purchase_table, metadata, autoload_with=olap_engine)

fct_booking, fct_purchase = get_facts(
    deleted_booking, deleted_booking_rooms, deleted_booking_addons
)

for row in fct_booking.to_dict(orient="records"):
    query = (
        select(FactBooking)
        .where(FactBooking.c.guest == row["guest"])
        .where(FactBooking.c.datetime >= int(row["checkin"].strftime(dt_fmt)))
        .where(FactBooking.c.datetime <= int(row["checkout"].strftime(dt_fmt)))
    )
    assert olap_conn.execute(query).fetchall() == []

for row in fct_purchase.to_dict(orient="records"):
    query = (
        select(FactPurchase)
        .where(FactPurchase.c.guest == row["guest"])
        .where(FactPurchase.c.datetime == int(row["datetime"].strftime(dt_fmt)))
        .where(FactPurchase.c.addon == row["addon"])
        .where(FactPurchase.c.addon_quantity == row["quantity"])
    )
    assert olap_conn.execute(query).fetchall() == []

olap_conn.close()
olap_engine.dispose()
