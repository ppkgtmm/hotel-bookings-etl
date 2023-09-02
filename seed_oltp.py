from datetime import timedelta
import random
from dotenv import load_dotenv
from os import getenv
import pandas as pd
from sqlalchemy import create_engine, text
from faker.generator import random
from warnings import filterwarnings

filterwarnings(action="ignore")

load_dotenv()

max_rooms = 5
max_addon_cnt = 10
max_addon_quantity = 3

db_host = getenv("DB_HOST")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_port = getenv("DB_PORT")
db_name = getenv("OLTP_DB")
location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
users_table = getenv("USERS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
booking_addons_table = getenv("BOOKING_ADDONS_TABLE")

data_dir = getenv("SEED_DIR")
seed = getenv("SEED")

random.seed(seed)

room_counts = list(range(1, max_rooms + 1))
sum_count = sum(room_counts)
count_weight = [(max_rooms - rc + 1) / sum_count for rc in room_counts]

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)


def load_location():
    location = pd.read_csv(data_dir + "location.csv")
    location.to_sql(location_table, conn, index=False, if_exists="append")


def load_room_types():
    room_types = pd.read_csv(data_dir + "room_types.csv")
    room_types.to_sql(roomtypes_table, conn, index=False, if_exists="append")


def load_addons():
    addons = pd.read_csv(data_dir + "addons.csv")
    addons.to_sql(addons_table, conn, index=False, if_exists="append")


def load_users():
    columns = ["firstname", "lastname", "gender", "email", "id"]
    users = pd.read_csv(data_dir + "users.csv")
    location = pd.read_sql_table(location_table, conn)
    merged = users.merge(location, on=["state", "country"])[columns]
    merged = merged.rename(columns={"id": "location"})
    merged.to_sql(users_table, conn, index=False, if_exists="append")


def load_guests():
    columns = ["firstname", "lastname", "gender", "email", "dob", "id"]
    guests = pd.read_csv(data_dir + "guests.csv")
    location = pd.read_sql_table(location_table, conn)
    merged = guests.merge(location, on=["state", "country"])[columns]
    merged = merged.rename(columns={"id": "location"})
    merged.to_sql(guests_table, conn, index=False, if_exists="append")


def load_rooms():
    columns = ["floor", "number", "id"]
    rooms = pd.read_csv(data_dir + "rooms.csv")
    room_types = pd.read_sql_table(roomtypes_table, conn)
    merged = rooms.merge(room_types, left_on="type", right_on="name")[columns]
    merged = merged.rename(columns={"id": "type"})
    merged.to_sql(rooms_table, conn, index=False, if_exists="append")


def load_bookings():
    insert_q = f"INSERT INTO {bookings_table} (user, checkin, checkout, payment) VALUES (:user, :checkin, :checkout, :payment)"
    columns = ["id", "checkin", "checkout", "payment"]
    bookings = pd.read_csv(data_dir + "bookings.csv")
    users = pd.read_sql_table(users_table, conn)
    addons = pd.read_sql_table(addons_table, conn)

    bookings["checkin"] = pd.to_datetime(bookings["checkin"])
    bookings["checkout"] = pd.to_datetime(bookings["checkout"])
    bookings["payment"] = pd.to_datetime(bookings["payment"])

    merged = bookings.merge(users, left_on="user", right_on="email")[columns]
    merged = merged.rename(columns={"id": "user"}).sort_values(by=["checkin"])

    room_types = pd.read_sql(f"SELECT id FROM {roomtypes_table}", conn)["id"].tolist()
    guests = pd.read_sql(f"SELECT id FROM {guests_table}", conn)["id"].tolist()

    for row in merged.to_dict(orient="records"):
        result = conn.execute(text(insert_q), row)
        booking = {**row, "id": result.lastrowid}
        load_booking_rooms(booking, room_types, guests, addons)


def load_booking_rooms(booking, room_types, guests, addons):
    booking_room_q = f"INSERT INTO {booking_rooms_table} (booking, room, guest) VALUES (:booking, :room, :guest)"
    room_q = "SELECT id, type FROM {} WHERE type IN ({})"
    checkin, checkout = booking["checkin"], booking["checkout"]
    overlapping = pd.read_sql(
        f"""
        SELECT guest, room
        FROM {booking_rooms_table}
        WHERE booking IN (
            SELECT id
            FROM {bookings_table}
            WHERE (checkin <= DATE('{checkin}') AND DATE('{checkin}') <= checkout)
            OR (checkin <= DATE('{checkout}')AND DATE('{checkout}') <= checkout)
            OR (DATE('{checkin}') <= checkin AND DATE('{checkout}') >= checkout)
        )
        """,
        conn,
    )
    num_rooms = random.choices(room_counts, weights=count_weight, k=1)[0]

    avail_guests = set(guests) - set(overlapping.guest.to_list())
    assert len(avail_guests) >= num_rooms
    guest = random.sample(avail_guests, k=num_rooms)

    room_type = random.choices(room_types, k=num_rooms)
    room_q = room_q.format(
        rooms_table, ",".join(pd.Series(room_type, dtype=str).tolist())
    )
    rooms = pd.read_sql(room_q, conn)
    room = []
    for type in room_type:
        avail_rooms = set(rooms[rooms["type"] == type]["id"])
        avail_rooms = avail_rooms - set(overlapping.room.to_list())
        assert len(avail_rooms) >= 1
        sample = random.sample(avail_rooms, k=1)
        room += sample
        rooms = rooms[~rooms["id"].isin(sample)]

    booking_room = {"booking": booking["id"], "room": room, "guest": guest}
    booking_rooms = pd.DataFrame([booking_room])
    booking_rooms = booking_rooms.explode(["room", "guest"])
    booking_rooms = booking_rooms.drop_duplicates("room").drop_duplicates("guest")
    for row in booking_rooms.to_dict(orient="records"):
        result = conn.execute(text(booking_room_q), row)
        booking_room = {**row, "id": result.lastrowid}
        load_booking_addons(booking, booking_room, addons)


def load_booking_addons(booking, booking_room, addons):
    booking_addons_q = f"INSERT INTO {booking_addons_table} (booking_room, addon, quantity, datetime) VALUES (:booking_room, :addon, :quantity, :datetime)"
    stay_duration = (booking["checkout"] - booking["checkin"]).days
    result = []
    for day in range(random.randint(0, stay_duration)):
        max_addons = random.randint(1, max_addon_cnt)
        chosen_addons = random.choices(addons["id"].tolist(), k=max_addons)
        for addon in chosen_addons:
            quantity = random.randint(1, max_addon_quantity)
            datetime = booking["checkin"] + timedelta(
                days=day,
                hours=random.randint(0, 23),
                minutes=random.choice([0, 30]),
            )
            assert booking["checkin"] <= datetime <= booking["checkout"]
            data = dict(
                booking_room=booking_room["id"],
                addon=addon,
                quantity=quantity,
                datetime=datetime,
            )
            result.append(data)
    if result == []:
        return
    df = pd.DataFrame(result)
    df = df.groupby(["booking_room", "addon", "datetime"]).aggregate("sum")
    for row in df.reset_index().to_dict(orient="records"):
        conn.execute(text(booking_addons_q), row)


if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()
    load_location()
    load_room_types()
    load_addons()
    load_users()
    load_guests()
    load_rooms()
    load_bookings()
    conn.close()
    engine.dispose()
