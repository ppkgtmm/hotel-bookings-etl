from faker import Faker
from faker.generator import random
import pandas as pd
from datetime import timedelta
from dotenv import load_dotenv
from os import getenv

load_dotenv()

data_dir = getenv("SEED_DIR")
seed = getenv("SEED")

users_file = data_dir + "users.csv"
guests_file = data_dir + "guests.csv"

fake = Faker()
Faker.seed(seed)

users = pd.read_csv(users_file)
guests = pd.read_csv(guests_file)
user_count = users.shape[0]


def generate_bookings(count: int, max_stay: int):
    stay_duration = list(range(1, max_stay + 1))
    max_stay, sum_stay = max(stay_duration), sum(stay_duration)
    stay_weight = [(max_stay - sd + 1) / sum_stay for sd in stay_duration]

    bookings = pd.DataFrame(
        [],
        columns=["user", "checkin", "checkout", "payment"],
    )

    for _ in range(count):
        checkin = fake.date_between(start_date="-21m", end_date="+3m")
        stay_days = random.choices(stay_duration, weights=stay_weight, k=1)[0]
        checkout = checkin + timedelta(days=stay_days)
        payment = fake.date_time_between(
            start_date=checkin + timedelta(days=-90),
            end_date=checkin + timedelta(days=-1),
        )
        user = users.loc[_ % user_count, "email"]
        booking = pd.DataFrame(
            [
                dict(
                    user=user,
                    checkin=checkin,
                    checkout=checkout,
                    payment=payment,
                )
            ]
        )
        bookings = pd.concat([bookings, booking])
    return bookings.reset_index(drop=True)


if __name__ == "__main__":
    num_bookings = 150
    max_stay = 15

    bookings = generate_bookings(num_bookings, max_stay)
    bookings.to_csv(data_dir + "bookings.csv", index=False)
