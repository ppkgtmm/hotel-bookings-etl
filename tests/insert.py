from typing import List
from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select, insert, delete
from common import *

load_dotenv()

addons_table = getenv("ADDONS_TABLE")
guests_table = getenv("GUESTS_TABLE")
rooms_table = getenv("ROOMS_TABLE")
users_table = getenv("USERS_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")


def insert_booking_addons(booking_rooms: List[dict], booking: dict):
    addon_table = Table(addons_table, MetaData(), autoload_with=oltp_engine)
    addon = oltp_conn.execute(select(addon_table.c.id)).fetchone()[0]
    date_time = datetime.fromisoformat(booking["checkin"].isoformat())
    date_time = date_time.replace(hour=14, minute=0, second=0) + timedelta(days=8)
    data = [
        dict(
            id=id,
            booking_room=booking_room["id"],
            addon=addon,
            datetime=date_time,
            quantity=1,
            updated_at=datetime.now(),
        )
        for id, booking_room in zip(booking_addon_ids, booking_rooms)
    ]

    table = Table(raw_booking_addon_table, MetaData(), autoload_with=dwh_engine)
    query = insert(table).values(data)
    dwh_conn.execute(query)
    dwh_conn.commit()
    return data


def insert_booking_room(booking: dict):
    room_table = Table(rooms_table, MetaData(), autoload_with=oltp_engine)
    guest_table = Table(guests_table, MetaData(), autoload_with=oltp_engine)
    room_ids = oltp_conn.execute(select(room_table.c.id)).fetchmany(2)
    guest_ids = oltp_conn.execute(select(guest_table.c.id)).fetchmany(2)
    table = Table(raw_booking_room_table, MetaData(), autoload_with=dwh_engine)
    data = [
        dict(
            id=id,
            booking=booking["id"],
            room=room[0],
            guest=guest[0],
            updated_at=datetime.now(),
        )
        for id, room, guest in zip(booking_room_ids, room_ids, guest_ids)
    ]
    query = insert(table).values(data)
    dwh_conn.execute(query)
    dwh_conn.commit()
    return data


def insert_booking():
    data = dict(
        id=booking_id,
        checkin=datetime.today().date() + timedelta(days=10),
        checkout=datetime.today().date() + timedelta(days=20),
        updated_at=datetime.now(),
    )
    table = Table(raw_booking_table, MetaData(), autoload_with=dwh_engine)
    query = insert(table).values(**data)
    dwh_conn.execute(query)
    dwh_conn.commit()
    return data


def pre_insert():
    booking_table = Table(raw_booking_table, MetaData(), autoload_with=dwh_engine)
    booking_room_table = Table(
        raw_booking_room_table, MetaData(), autoload_with=dwh_engine
    )
    booking_addon_table = Table(
        raw_booking_addon_table, MetaData(), autoload_with=dwh_engine
    )
    queries = [
        delete(booking_table).where(booking_table.c.id == booking_id),
        delete(booking_room_table).where(booking_room_table.c.id.in_(booking_room_ids)),
        delete(booking_addon_table).where(
            booking_addon_table.c.id.in_(booking_addon_ids)
        ),
    ]
    for query in queries:
        dwh_conn.execute(query)
        dwh_conn.commit()


if __name__ == "__main__":
    conn_str_map = get_connection_str()
    oltp_engine = create_engine(conn_str_map.get("oltp"))
    dwh_engine = create_engine(conn_str_map.get("dwh"))
    oltp_conn = oltp_engine.connect()
    dwh_conn = dwh_engine.connect()

    pre_insert()
    booking = insert_booking()
    booking_rooms = insert_booking_room(booking)
    booking_addons = insert_booking_addons(booking_rooms, booking)

    pd.DataFrame([booking]).to_csv(booking_file, index=False)
    pd.DataFrame(booking_rooms).to_csv(booking_room_file, index=False)
    pd.DataFrame(booking_addons).to_csv(booking_addon_file, index=False)

    oltp_conn.close()
    dwh_conn.close()
    oltp_engine.dispose()
    dwh_engine.dispose()
