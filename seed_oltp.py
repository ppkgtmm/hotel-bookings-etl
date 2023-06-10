from datetime import timedelta
import math
import random
from dotenv import load_dotenv
from os import getenv
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

db_password_key = "DB_PASSWORD"

db_host = "localhost"
db_user = "root"
db_password = getenv(db_password_key)
db_name = "oltp_hotel"

data_dir = "seeds/"
connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_name}"
)


def load_person():
    users = pd.read_csv(data_dir + "users.csv")
    guests = pd.read_csv(data_dir + "guests.csv")
    users.to_sql("users", conn, index=False, if_exists="append")
    guests.to_sql("guests", conn, index=False, if_exists="append")


def load_static_data():
    room_types = pd.read_csv(data_dir + "room_types.csv")
    addons = pd.read_csv(data_dir + "addons.csv")
    room_types.to_sql("roomtypes", conn, index=False, if_exists="append")
    addons.to_sql("addons", conn, index=False, if_exists="append")


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
        SELECT DISTINCT u.id user, g.id guest, checkin, checkout, payment
        FROM bookings_temp b
        LEFT JOIN users u
        ON b.user = u.email
        LEFT JOIN guests g
        ON b.guest = g.email
        ORDER BY 5
        """,
        conn,
    )
    bookings_merged.to_sql("bookings", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE bookings_temp"))


def load_booking_rooms():
    pd.read_csv(data_dir + "bookings.csv").to_sql(
        "bookings_temp", conn, index=False, if_exists="replace"
    )
    conn.execute(text("DELETE FROM booking_rooms;"))
    conn.execute(text("ALTER TABLE booking_rooms AUTO_INCREMENT = 1;"))
    room_preference = conn.execute(
        text(
            """
            SELECT b.id booking, b.checkin, b.checkout, bt.room_type, bt.min_floor, FLOOR(SUM(bt.count)) room_cnt
            FROM bookings_temp bt
            LEFT JOIN users u
            ON bt.user = u.email
            LEFT JOIN bookings b
            ON b.payment = bt.payment AND b.user = u.id
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY 1
            """
        )
    )
    for row in room_preference:
        rooms = conn.execute(
            text(
                f"""
                SELECT DISTINCT {row.booking} booking, r.id room
                FROM rooms r
                    INNER JOIN (
                        SELECT id
                        FROM roomtypes
                        WHERE name = '{row.room_type}'
                    ) rt
                ON r.type = rt.id AND r.floor >= {row.min_floor}
                LEFT JOIN booking_rooms br
                ON br.room = r.id
                LEFT JOIN bookings eb
                ON br.booking = eb.id AND (
                (DATE('{row.checkin}') BETWEEN eb.checkin AND eb.checkout)
                OR (DATE('{row.checkout}') BETWEEN eb.checkin AND eb.checkout)
                OR (DATE('{row.checkin}') <= eb.checkin AND DATE('{row.checkout}') >= eb.checkout)
                )
                WHERE eb.id IS NULL
                ORDER BY 1
                LIMIT {row.room_cnt};
            """
            )
        )
        for room in rooms:
            conn.execute(
                text(
                    f"INSERT INTO booking_rooms (booking, room) VALUES ({room.booking}, {room.room})"
                )
            )
    conn.execute(text("DROP TABLE bookings_temp;"))


def load_booking_room_addons():
    addon_df = pd.read_sql("SELECT * FROM addons", conn)
    addons = addon_df["name"].unique()
    booking_rooms = conn.execute(
        text(
            """
            SELECT br.*, b.checkin, b.checkout, DATEDIFF(b.checkout, b.checkin) day_diff
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
                date = br.checkin + timedelta(days=day)
                assert br.checkin <= date <= br.checkout
                result.append(
                    dict(
                        bookingrooms=br.id,
                        addon=addon_df[addon_df["name"] == chosen_addon]["id"].iloc[0],
                        quantity=quantity,
                        date=date,
                    )
                )
    pd.DataFrame(result).to_sql(
        "booking_room_addons_temp", conn, index=False, if_exists="replace"
    )
    pd.read_sql(
        """
        SELECT bookingrooms, addon, FLOOR(SUM(quantity)) quantity, date
        FROM booking_room_addons_temp
        GROUP BY 1, 2, 4
        ORDER BY 1
        """,
        conn,
    ).to_sql("booking_room_addons", conn, index=False, if_exists="append")
    conn.execute(text("DROP TABLE booking_room_addons_temp;"))


if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()
    load_person()
    load_static_data()
    load_rooms()
    load_bookings()
    load_booking_rooms()
    load_booking_room_addons()
    conn.close()
    engine.dispose()
