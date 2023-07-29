from datetime import timedelta
import random
from dotenv import load_dotenv
from os import getenv
import pandas as pd
from sqlalchemy import create_engine, text
from faker.generator import random

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

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_name}"
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
    users = pd.read_csv(data_dir + "users.csv")
    users.to_sql("users_temp", conn, index=False, if_exists="append")
    users_merged = pd.read_sql(
        """
        SELECT u.firstname, u.lastname, u.gender, u.email, l.id location
        FROM users_temp u
        LEFT JOIN location l
        ON u.state = l.state AND u.country = l.country
        """,
        conn,
    )
    users_merged.to_sql("users", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE users_temp"))


def load_guests():
    guests = pd.read_csv(data_dir + "guests.csv")
    guests.to_sql("guests_temp", conn, index=False, if_exists="append")
    guests_merged = pd.read_sql(
        """
        SELECT g.firstname, g.lastname, g.gender, g.email, g.dob, l.id location
        FROM guests_temp g
        LEFT JOIN location l
        ON g.state = l.state AND g.country = l.country
        """,
        conn,
    )
    guests_merged.to_sql("guests", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE guests_temp"))


def load_rooms():
    pd.read_csv(data_dir + "rooms.csv").to_sql(
        "rooms_temp", conn, index=False, if_exists="replace"
    )
    rooms_merged = pd.read_sql(
        """
        SELECT r.floor, r.number, rt.id `type`
        FROM rooms_temp r
        LEFT JOIN roomtypes rt
        ON r.type = rt.name
        ORDER BY 1,2
        """,
        conn,
    )
    rooms_merged.to_sql("rooms", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE rooms_temp"))


def load_bookings():
    pd.read_csv(data_dir + "bookings.csv").to_sql(
        "bookings_temp", conn, index=False, if_exists="replace"
    )
    bookings_merged = pd.read_sql(
        """
        SELECT DISTINCT u.id user, checkin, checkout, payment
        FROM bookings_temp b
        LEFT JOIN users u
        ON b.user = u.email
        ORDER BY 4
        """,
        conn,
    )
    bookings_merged.to_sql("bookings", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE bookings_temp"))


def load_booking_rooms():
    room_counts = list(range(1, max_rooms + 1))
    sum_count = sum(room_counts)
    count_weight = [(max_rooms - rc + 1) / sum_count for rc in room_counts]
    bookings = pd.read_sql("SELECT id, checkin, checkout FROM bookings", conn)
    room_types = [rt[0] for rt in conn.execute(text("SELECT id FROM roomtypes"))]
    guests = set([g[0] for g in conn.execute(text("SELECT id FROM guests"))])
    booking_rooms = []
    for booking in bookings.to_dict(orient="records"):
        checkin, checkout = booking["checkin"], booking["checkout"]
        overlapping = pd.read_sql(
            f"""
            SELECT guest, room, type
            FROM booking_rooms br
            INNER JOIN bookings b
            ON b.id = br.booking
            INNER JOIN rooms r
            ON br.room = r.id
            WHERE (b.checkin <= {checkin} AND {checkin} <= b.checkout)
            OR (b.checkin <= {checkout} AND {checkout} <= b.checkout)
            OR ({checkin} <= b.checkin AND {checkout} >= b.checkout)
            """,
            conn,
        )
        num_rooms = random.choices(room_counts, weights=count_weight, k=1)[0]
        guest, avail_guests = [], guests - set(overlapping.guest.to_list())
        for _ in range(num_rooms):
            avail_guests = avail_guests - set(guest)
            assert len(avail_guests) >= 1
            guest.append(random.choice(list(avail_guests)))
        room_type = random.choices(room_types, k=num_rooms)
        room_query = f"SELECT id FROM rooms WHERE type IN ({', '.join([str(rt) for rt in room_type])})"
        rooms = set([r[0] for r in conn.execute(text(room_query))])
        assigned_rooms = []
        for type in room_type:
            booked_rooms = overlapping[overlapping.type == type].room.to_list()
            avail_rooms = list(rooms - set(booked_rooms) - set(assigned_rooms))
            assert len(avail_rooms) >= 1
            assigned_rooms.append(random.choice(avail_rooms))
        assigned_rooms, guest = list(set(assigned_rooms)), list(set(guest))
        num_deduped = min(len(assigned_rooms), len(guest))
        booking_rooms.append(
            {
                "booking": booking["id"],
                "room": assigned_rooms[0:num_deduped],
                "guest": guest[0:num_deduped],
            }
        )
    booking_rooms = pd.DataFrame(booking_rooms)
    booking_rooms = booking_rooms.explode(["room", "guest"])
    booking_rooms.to_sql("booking_rooms", conn, index=False, if_exists="append")
    conn.commit()


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
