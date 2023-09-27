from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData, select, insert
from common import get_connection_str, get_insert_query

load_dotenv()

# location_table = getenv("LOCATION_TABLE")

# addons_table = getenv("ADDONS_TABLE")
# roomtypes_table = getenv("ROOMTYPES_TABLE")
guests_table = getenv("GUESTS_TABLE")
rooms_table = getenv("ROOMS_TABLE")
users_table = getenv("USERS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
# booking_addons_table = getenv("BOOKING_ADDONS_TABLE")


booking = dict(
    checkin=datetime.today() + timedelta(days=10),
    checkout=datetime.today() + timedelta(days=15),
    payment=datetime.now(),
)


def insert_booking_room(booking: int):
    table = Table(booking_rooms_table, MetaData(), autoload_with=oltp_engine)
    room_table = Table(rooms_table, MetaData(), autoload_with=oltp_engine)
    guest_table = Table(guests_table, MetaData(), autoload_with=oltp_engine)
    room_query = select(room_table.c.id).limit(2)
    guest_query = select(guest_table.c.id).limit(2)
    room_ids = oltp_conn.execute(room_query).fetchall()
    guest_ids = oltp_conn.execute(guest_query).fetchall()
    ids = []
    for room, guest in zip(room_ids, guest_ids):
        data = {"booking": booking, "room": room[0], "guest": guest[0]}
        query = insert(table).values(**data)
        result = oltp_conn.execute(query)
        oltp_conn.commit()
        ids.append(result.inserted_primary_key)
    return ids


def insert_booking():
    table = Table(bookings_table, MetaData(), autoload_with=oltp_engine)
    user_table = Table(users_table, MetaData(), autoload_with=oltp_engine)
    user_query = select(user_table.c.id).limit(1)
    user_id = oltp_conn.execute(user_query).fetchone()[0]
    data = dict(**booking, user=user_id)
    query = insert(table).values(**data)
    result = oltp_conn.execute(query)
    oltp_conn.commit()
    return result.inserted_primary_key


if __name__ == "__main__":
    conn_str_map = get_connection_str()
    oltp_engine = create_engine(conn_str_map.get("oltp"))
    olap_engine = create_engine(conn_str_map.get("olap"))
    oltp_conn = oltp_engine.connect()
    olap_conn = olap_engine.connect()

    booking_id = insert_booking()[0]
    booking_room_ids = insert_booking_room(booking_id)

    print(booking_id, booking_room_ids)

    oltp_conn.close()
    olap_conn.close()
    oltp_engine.dispose()
    olap_engine.dispose()
