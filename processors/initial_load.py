from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from os import getenv
from constants import *
from db_writer import DatabaseWriter

load_dotenv()
db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
oltp_db = getenv("OLTP_DB")
location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
booking_addons_table = getenv("BOOKING_ADDONS_TABLE")

conn_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    db_user, db_password, db_host, db_port, oltp_db
)

batch_size = 1000


def get_data(query):
    cursor = oltp_conn.execute(text(query))
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            return
        yield rows


def load_locations():
    for locations in get_data(location_query):
        locations = [
            {"id": row[0], "state": row[1], "country": row[2]} for row in locations
        ]
        writer.write_dim_locations(locations)


def load_addons():
    for addons in get_data(addon_query):
        addons = [
            {"_id": row[0], "name": row[1], "price": row[2], "created_at": row[3]}
            for row in addons
        ]
        writer.write_dim_addons(addons)


def load_roomtypes():
    for roomtypes in get_data(roomtype_query):
        roomtypes = [
            {"_id": row[0], "name": row[1], "price": row[2], "created_at": row[3]}
            for row in roomtypes
        ]
        writer.write_dim_roomtypes(roomtypes)


def load_rooms():
    for rooms in get_data(room_query):
        rooms = [{"id": row[0], "type": row[1], "updated_at": row[2]} for row in rooms]
        writer.stage_rooms(rooms)


def load_guests():
    for guests in get_data(guest_query):
        guests = [
            {
                "id": row[0],
                "email": row[1],
                "dob": row[2],
                "gender": row[3],
                "location": row[4],
                "updated_at": row[5],
            }
            for row in guests
        ]
        writer.stage_guests(guests)
        guests = [
            {
                "id": row["id"],
                "email": row["email"],
                "dob": row["dob"],
                "gender": row["gender"],
            }
            for row in guests
        ]
        writer.write_dim_guests(guests)


def stage_bookings():
    for bookings in get_data(booking_query):
        bookings = [
            {"id": row[0], "checkin": row[1], "checkout": row[2]} for row in bookings
        ]
        writer.stage_bookings(bookings)


def stage_booking_rooms():
    for booking_rooms in get_data(booking_room_query):
        booking_rooms = [
            {
                "id": row[0],
                "booking": row[1],
                "room": row[2],
                "guest": row[3],
                "updated_at": row[4],
            }
            for row in booking_rooms
        ]
        writer.stage_booking_rooms(booking_rooms)


def stage_booking_addons():
    for booking_addons in get_data(booking_addon_query):
        booking_addons = [
            {
                "id": row[0],
                "booking_room": row[1],
                "addon": row[2],
                "quantity": row[3],
                "datetime": row[4],
                "updated_at": row[5],
            }
            for row in booking_addons
        ]
        writer.stage_booking_addons(booking_addons)


if __name__ == "__main__":
    oltp_engine = create_engine(conn_string, isolation_level="SERIALIZABLE")
    oltp_conn = oltp_engine.connect()
    writer = DatabaseWriter()
    stage_booking_addons()
    stage_booking_rooms()
    stage_bookings()
    load_locations()
    load_addons()
    load_roomtypes()
    load_guests()
    load_rooms()
    writer.write_fct_bookings()
    writer.write_fct_purchases()
    writer.tear_down()
    oltp_conn.close()
    oltp_engine.dispose()
