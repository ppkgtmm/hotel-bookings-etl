from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update
from common import *

load_dotenv()

addons_table = getenv("ADDONS_TABLE")
guests_table = getenv("GUESTS_TABLE")
rooms_table = getenv("ROOMS_TABLE")
users_table = getenv("USERS_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")


def update_booking_addons(booking_addons: pd.DataFrame):
    table = Table(raw_booking_addon_table, MetaData(), autoload_with=dwh_engine)
    data = []
    for booking_addon in booking_addons.to_dict(orient="records"):
        id, date_time = booking_addon["id"], booking_addon.pop("datetime")
        date_time = datetime.fromisoformat(date_time) - timedelta(days=5)
        query = update(table).where(table.c.id == id).values(datetime=date_time)
        dwh_conn.execute(query)
        dwh_conn.commit()
        data.append(dict(**booking_addon, datetime=date_time))
    return data


def update_booking(bookings: pd.DataFrame):
    table = Table(raw_booking_table, MetaData(), autoload_with=dwh_engine)
    data = []
    for booking in bookings.to_dict(orient="records"):
        id = booking["id"]
        checkin = booking.pop("checkin")
        checkout = booking.pop("checkout")
        checkin = datetime.fromisoformat(checkin) - timedelta(days=5)
        checkout = datetime.fromisoformat(checkout) - timedelta(days=5)
        query = (
            update(table)
            .where(table.c.id == id)
            .values(checkin=checkin.date(), checkout=checkout.date())
        )
        dwh_conn.execute(query)
        dwh_conn.commit()
        data.append(dict(**booking, checkin=checkin.date(), checkout=checkout.date()))
    return data


def pre_update():
    bookings = pd.read_csv(booking_file)
    booking_addons = pd.read_csv(booking_addon_file)
    return bookings, booking_addons


if __name__ == "__main__":
    conn_str_map = get_connection_str()
    dwh_engine = create_engine(conn_str_map.get("dwh"))
    dwh_conn = dwh_engine.connect()

    bookings, booking_addons = pre_update()
    updated_bookings = update_booking(bookings)
    updated_booking_addons = update_booking_addons(booking_addons)

    pd.DataFrame(updated_bookings).to_csv(booking_file, index=False)
    pd.DataFrame(updated_booking_addons).to_csv(booking_addon_file, index=False)

    dwh_conn.close()
    dwh_engine.dispose()
