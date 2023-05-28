from faker import Faker
from faker.generator import random
import pandas as pd
from datetime import date, timedelta
from room import weights, room_types, floors
import math

data_dir = "data/"
users_file = "data/person/users.csv"
guests_file = "data/person/guests.csv"

fake = Faker()
users = pd.read_csv(users_file)
guests = pd.read_csv(guests_file)
user_count = users.shape[0]


def generate_room_preference(types_counts: list, room_counts: list):
    types_count = random.choice(types_counts)
    rooms = []
    for type in random.choices(
        room_types.name.tolist(), weights=weights, k=types_count
    ):
        rooms.append(
            dict(
                type=type,
                count=random.choice(room_counts),
                min_floor=math.floor(random.random() * (floors - 1))
                + 1,  # min floor allowed from 1 to total floors - 1
            )
        )
    return rooms


def generate_bookings(
    count: int, max_stay: int, max_types: int, max_count_per_type: int
):
    stay_duration = list(range(1, max_stay + 1))
    max_stay, sum_stay = max(stay_duration), sum(stay_duration)
    stay_weight = [(max_stay - sd + 1) / sum_stay for sd in stay_duration]

    types = list(range(1, max_types + 1))
    room_counts = list(range(1, max_count_per_type + 1))

    bookings = pd.DataFrame(
        [],
        columns=["user", "guest", "checkin", "checkout", "payment", "rooms_preference"],
    )

    for _ in range(count):
        checkin = fake.date_between(
            start_date=date(year=2021, month=1, day=1), end_date="today"
        )
        stay_days = random.choices(stay_duration, weights=stay_weight, k=1)[0]
        checkout = checkin + timedelta(days=stay_days)
        payment = fake.date_time_between(
            start_date=checkin + timedelta(days=-90),
            end_date=checkin + timedelta(days=-1),
        )
        user = users.loc[_ % user_count, "email"]
        if bookings.shape[0] == 0:
            guest = random.choice(guests.email)
        else:
            if random.random() >= 0.65:
                guest = random.choice(guests.email)
            else:
                guest = user
            overlap_count = bookings[
                (bookings["guest"] == guest)
                & (
                    (
                        (bookings["checkin"] <= checkin)
                        & (checkin <= bookings["checkout"])
                    )
                    | (
                        (bookings["checkin"] <= checkout)
                        & (checkout <= bookings["checkout"])
                    )
                    | (
                        (checkin <= bookings["checkin"])
                        & (checkout >= bookings["checkout"])
                    )
                )
            ].shape[0]
            if overlap_count > 0:
                continue
        rooms_preference = generate_room_preference(types, room_counts)
        booking = pd.DataFrame(
            [
                dict(
                    user=user,
                    guest=guest,
                    checkin=checkin,
                    checkout=checkout,
                    payment=payment,
                    rooms_preference=rooms_preference,
                )
            ]
        )
        bookings = pd.concat([bookings, booking])
    return bookings.reset_index(drop=True)


if __name__ == "__main__":
    num_bookings = 85
    max_stay = 20
    max_types = 3
    max_count_per_type = 2

    bookings = generate_bookings(num_bookings, max_stay, max_types, max_count_per_type)

    sort = bookings.sort_values(["guest", "checkin"])
    shifted = sort.join(sort[["guest", "checkout"]].shift(), rsuffix="_prev")
    assert shifted.shape[0] == bookings.shape[0]
    assert (
        shifted[
            (shifted["guest"] == shifted["guest_prev"])
            & (
                shifted["checkout_prev"].between(
                    shifted["checkin"], shifted["checkout"]
                )
            )
        ].shape[0]
        == 0
    )
    bookings = bookings.explode("rooms_preference")
    bookings[["room_type", "count", "min_floor"]] = bookings["rooms_preference"].apply(
        pd.Series
    )
    bookings.drop(columns=["rooms_preference"], inplace=True)
    bookings.to_csv(data_dir + "bookings.csv", index=False)
