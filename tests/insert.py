from typing import List
from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData, select, insert
from common import get_connection_str

booking_id = 100000001
booking_room_id = [100000001, 100000002]
booking_addon_id = [100000001, 100000002]

load_dotenv()

# location_table = getenv("LOCATION_TABLE")
# roomtypes_table = getenv("ROOMTYPES_TABLE")

addons_table = getenv("ADDONS_TABLE")
guests_table = getenv("GUESTS_TABLE")
rooms_table = getenv("ROOMS_TABLE")
users_table = getenv("USERS_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
# booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
# booking_addons_table = getenv("BOOKING_ADDONS_TABLE")


# def insert_booking_addons(booking_rooms: List[int]):
#     table = Table(booking_addons_table, MetaData(), autoload_with=oltp_engine)
#     addon_table = Table(addons_table, MetaData(), autoload_with=oltp_engine)
#     addon_query = select(addon_table.c.id).limit(1)
#     addon = oltp_conn.execute()
#     # for booking_room in booking_rooms:


def insert_booking_room(booking: dict):
    room_table = Table(rooms_table, MetaData(), autoload_with=oltp_engine)
    guest_table = Table(guests_table, MetaData(), autoload_with=oltp_engine)
    room_ids = oltp_conn.execute(select(room_table.c.id)).fetchmany(2)
    guest_ids = oltp_conn.execute(select(guest_table.c.id)).fetchmany(2)
    table = Table(raw_booking_room_table, MetaData(), autoload_with=olap_engine)
    data = [
        dict(
            id=id,
            booking=booking["id"],
            room=room[0],
            guest=guest[0],
            updated_at=datetime.now(),
        )
        for id, room, guest in zip(booking_room_id, room_ids, guest_ids)
    ]
    query = insert(table).values(data)
    olap_conn.execute(query)
    olap_conn.commit()
    return data


def insert_booking():
    data = dict(
        id=booking_id,
        checkin=datetime.today() + timedelta(days=10),
        checkout=datetime.today() + timedelta(days=15),
        updated_at=datetime.now(),
    )
    table = Table(raw_booking_table, MetaData(), autoload_with=olap_engine)
    query = insert(table).values(**data)
    olap_conn.execute(query)
    olap_conn.commit()
    return data


if __name__ == "__main__":
    conn_str_map = get_connection_str()
    oltp_engine = create_engine(conn_str_map.get("oltp"))
    olap_engine = create_engine(conn_str_map.get("olap"))
    oltp_conn = oltp_engine.connect()
    olap_conn = olap_engine.connect()

    booking = insert_booking()
    print(booking)
    booking_rooms = insert_booking_room(booking)
    print(booking_rooms)

    oltp_conn.close()
    olap_conn.close()
    oltp_engine.dispose()
    olap_engine.dispose()
