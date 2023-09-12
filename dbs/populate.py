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

overlapping_booking_sql = f"""
    SELECT guest, room
    FROM {booking_rooms_table} br
    INNER JOIN {bookings_table} b
    ON br.booking = b.id AND (
        (checkin <= DATE('{{checkin}}') AND DATE('{{checkin}}') <= checkout) OR
        (checkin <= DATE('{{checkout}}')AND DATE('{{checkout}}') <= checkout) OR
        (DATE('{{checkin}}') <= checkin AND DATE('{{checkout}}') >= checkout)
    )
    """


def load_room_types():
    room_types = pd.read_csv(data_dir + "room_types.csv")
    room_types.to_sql(roomtypes_table, conn, index=False, if_exists="append")


def load_addons():
    addons = pd.read_csv(data_dir + "addons.csv")
    addons.to_sql(addons_table, conn, index=False, if_exists="append")


def load_users():
    users = pd.read_csv(data_dir + "users.csv")
    users.to_sql(users_table, conn, index=False, if_exists="append")


def load_guests():
    guests = pd.read_csv(data_dir + "guests.csv")
    guests.to_sql(guests_table, conn, index=False, if_exists="append")


def load_rooms():
    columns = ["floor", "number", "id"]
    rooms = pd.read_csv(data_dir + "rooms.csv")
    room_types = pd.read_sql_table(roomtypes_table, conn)
    merged = rooms.merge(room_types, left_on="type", right_on="name")[columns]
    merged = merged.rename(columns={"id": "type"})
    merged.to_sql(rooms_table, conn, index=False, if_exists="append")


def load_bookings():
    columns = ["id", "checkin", "checkout", "payment"]
    bookings = pd.read_csv(data_dir + "bookings.csv")
    users = pd.read_sql_table(users_table, conn)

    bookings["checkin"] = pd.to_datetime(bookings["checkin"])
    bookings["checkout"] = pd.to_datetime(bookings["checkout"])
    bookings["payment"] = pd.to_datetime(bookings["payment"])

    merged = bookings.merge(users, left_on="user", right_on="email")[columns]
    merged = merged.rename(columns={"id": "user"}).sort_values(by=["checkin"])

    merged.to_sql(bookings_table, conn, index=False, if_exists="append")


def get_booking_details():
    guests = pd.read_sql_table(guests_table, conn, columns=["id"]).id.tolist()
    rooms = pd.read_sql(rooms_table, conn, columns=["id"]).id.tolist()
    bookings = pd.read_sql_table(bookings_table, conn)

    for booking in bookings.to_dict(orient="records"):
        checkin, checkout = booking["checkin"], booking["checkout"]
        overlapping = pd.read_sql(
            overlapping_booking_sql.format(checkin=checkin, checkout=checkout),
            conn,
        )
        num_rooms = random.choices(room_counts, weights=count_weight, k=1)[0]
        available_guests = set(guests) - set(overlapping.guest.to_list())
        available_rooms = set(rooms) - set(overlapping.room.to_list())
        assert len(available_guests) >= num_rooms
        guest = random.sample(available_guests, k=num_rooms)
        assert len(available_rooms) >= num_rooms
        room = random.sample(available_rooms, k=num_rooms)

        yield booking["id"], room, guest


def load_booking_rooms():
    for booking_detail in get_booking_details():
        booking, room, guest = booking_detail
        booking_room = {"booking": booking, "room": room, "guest": guest}
        booking_room = pd.DataFrame([booking_room]).explode(["room", "guest"])
        booking_room.to_sql(booking_rooms_table, conn, index=False, if_exists="append")
        conn.commit()


def load_booking_addons(booking, booking_room, addons):
    booking_addons_q = f"INSERT INTO {booking_addons_table} (booking_room, addon, quantity, datetime) VALUES (:booking_room, :addon, :quantity, :datetime)"
    stay_duration = (booking["checkout"] - booking["checkin"]).days
    result = []
    for day in range(random.randint(0, stay_duration)):
        max_addons = random.randint(1, max_addon_cnt)
        chosen_addons = random.choices(addons, k=max_addons)
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
        conn.commit()


if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()
    load_room_types()
    load_addons()
    load_users()
    load_guests()
    load_rooms()
    load_bookings()
    conn.close()
    engine.dispose()
