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

db_host = getenv("DB_HOST")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_port = getenv("DB_PORT")
db_name = getenv("OLTP_DB")

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
    location.to_sql("location", conn, index=False, if_exists="append")


def load_room_types():
    room_types = pd.read_csv(data_dir + "room_types.csv")
    room_types.to_sql("roomtypes", conn, index=False, if_exists="append")


def load_addons():
    addons = pd.read_csv(data_dir + "addons.csv")
    addons.to_sql("addons", conn, index=False, if_exists="append")


def load_users():
    columns = ["firstname", "lastname", "gender", "email", "id"]
    users = pd.read_csv(data_dir + "users.csv")
    location = pd.read_sql_table("location", conn)
    merged = users.merge(location, on=["state", "country"])[columns]
    merged = merged.rename(columns={"id": "location"})
    merged.to_sql("users", conn, index=False, if_exists="append")


def load_guests():
    columns = ["firstname", "lastname", "gender", "email", "dob", "id"]
    guests = pd.read_csv(data_dir + "guests.csv")
    location = pd.read_sql_table("location", conn)
    merged = guests.merge(location, on=["state", "country"])[columns]
    merged = merged.rename(columns={"id": "location"})
    merged.to_sql("guests", conn, index=False, if_exists="append")


def load_rooms():
    columns = ["floor", "number", "id"]
    rooms = pd.read_csv(data_dir + "rooms.csv")
    room_types = pd.read_sql_table("roomtypes", conn)
    merged = rooms.merge(room_types, left_on="type", right_on="name")[columns]
    merged = merged.rename(columns={"id": "type"})
    merged.to_sql("rooms", conn, index=False, if_exists="append")


def load_bookings():
    insert_q = "INSERT INTO bookings (user, checkin, checkout, payment) VALUES (:user, :checkin, :checkout, :payment)"
    columns = ["id", "checkin", "checkout", "payment"]
    bookings = pd.read_csv(data_dir + "bookings.csv")
    users = pd.read_sql_table("users", conn)
    merged = bookings.merge(users, left_on="user", right_on="email")[columns]
    merged = merged.rename(columns={"id": "user"})

    room_types = pd.read_sql("SELECT id FROM roomtypes", conn)["id"].tolist()
    guests = pd.read_sql("SELECT id FROM guests", conn)["id"].tolist()

    for row in merged.to_dict(orient="records"):
        result = conn.execute(text(insert_q), row)
        conn.commit()
        booking = {**row, "id": result.lastrowid}
        load_booking_rooms(booking, room_types, guests)


def load_booking_rooms(booking, room_types, guests):
    # booking_room_q = "INSERT INTO booking_rooms (booking, room, guest) VALUES (:booking, :room, :guest)"
    room_q = "SELECT id, type FROM rooms WHERE type IN ({})"
    checkin, checkout = booking["checkin"], booking["checkout"]
    overlapping = pd.read_sql(
        f"""
        SELECT guest, room
        FROM booking_rooms
        WHERE booking IN (
            SELECT id
            FROM bookings
            WHERE (checkin <= {checkin} AND {checkin} <= checkout)
            OR (checkin <= {checkout} AND {checkout} <= checkout)
            OR ({checkin} <= checkin AND {checkout} >= checkout)
        )
        """,
        conn,
    )
    num_rooms = random.choices(room_counts, weights=count_weight, k=1)[0]

    avail_guests = set(guests) - set(overlapping.guest.to_list())
    assert len(avail_guests) >= num_rooms
    guest = random.sample(avail_guests, k=num_rooms)

    room_type = random.choices(room_types, k=num_rooms)
    room_q = room_q.format(",".join(pd.Series(room_type, dtype=str).tolist()))
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
    booking_rooms.to_sql("booking_rooms", conn, index=False, if_exists="append")
def load_booking_room_addons():
    addon_df = pd.read_sql("SELECT * FROM addons", conn)
    addons = addon_df["name"].unique()
    booking_rooms = conn.execute(
        text(
            """
            SELECT br.*, CAST(b.checkin AS DATETIME) checkin, CAST(b.checkout AS DATETIME) checkout, DATEDIFF(b.checkout, b.checkin) day_diff
            FROM booking_rooms br
            LEFT JOIN bookings b
            ON br.booking = b.id
            ORDER BY br.booking
        """
        )
    )
    result = []
    for br in booking_rooms:
        for day in range(random.randint(0, br.day_diff)):
            max_distinct_addons = random.randint(1, addon_df.shape[0])
            for _ in range(max_distinct_addons):
                chosen_addon = random.choices(addons, k=1)[0]
                quantity = random.randint(1, 3)
                datetime = br.checkin + timedelta(
                    days=day,
                    hours=random.randint(0, 23),
                    minutes=random.choice([0, 30]),
                )
                assert br.checkin <= datetime <= br.checkout
                result.append(
                    dict(
                        booking_room=br.id,
                        addon=addon_df[addon_df["name"] == chosen_addon]["id"].iloc[0],
                        quantity=quantity,
                        datetime=datetime,
                    )
                )
    pd.DataFrame(result).to_sql(
        "booking_addons_temp", conn, index=False, if_exists="replace"
    )
    pd.read_sql(
        """
        SELECT booking_room, addon, FLOOR(SUM(quantity)) quantity, datetime
        FROM booking_addons_temp
        GROUP BY 1, 2, 4
        ORDER BY 1
        """,
        conn,
    ).to_sql("booking_addons", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE booking_addons_temp;"))
    # for booking_room in booking_rooms.to_dict(orient="records"):
    #     conn.execute(text(booking_room_q), booking_room)
    #     conn.commit()




if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()
    load_location()
    load_static_data()
    load_person()
    load_rooms()
    load_bookings()
    load_booking_rooms()
    load_booking_room_addons()
    conn.close()
    engine.dispose()
