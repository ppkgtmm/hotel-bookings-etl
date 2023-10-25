import pandas as pd
from faker import Faker
from faker.generator import random
from datetime import timedelta
from dotenv import load_dotenv
from os import getenv, path
from constants import *


class DataGenerator:
    def __init__(self, seed: str, genders: list[str] = None):
        self.seed = seed
        self.fake = Faker()
        Faker.seed(self.seed)
        self.genders = genders if genders else ["M", "F", "Prefer not to say"]

    @staticmethod
    def to_dataframe(data: list[dict]):
        return pd.DataFrame(data)

    @staticmethod
    def get_locations():
        url = getenv("LOCATION_FILE")
        locations = pd.read_csv(url)
        states, countries = locations["name"], locations["admin"]
        return zip(states.tolist(), countries.tolist())

    def generate_rooms(self, floors: int, floor_rooms: int):
        result = []
        room_types = DataGenerator.to_dataframe(room_variations)
        room_types = room_types.sort_values(by="price", ascending=False)
        room_types = room_types.reset_index(drop=True)
        weights = [(i + 1) / room_types.shape[0] for i in room_types.index]

        for room_number in range(1, floor_rooms + 1):
            room_type = random.choices(room_types.name.tolist(), weights=weights, k=1)
            for floor in range(1, floors + 1):
                result.append(dict(floor=floor, number=room_number, type=room_type[0]))

        return result

    def generate_bookings(self, count: int, max_stay: int):
        bookings = []
        stay_duration = list(range(1, max_stay + 1))
        max_stay, sum_stay = max(stay_duration), sum(stay_duration)
        stay_weight = [(max_stay - stay + 1) / sum_stay for stay in stay_duration]

        for _ in range(count):
            checkin = self.fake.date_between(start_date="-1y", end_date="today")
            stay_days = random.choices(stay_duration, weights=stay_weight, k=1)[0]
            checkout = checkin + timedelta(days=stay_days)
            payment = checkin - timedelta(days=random.randint(7, 30))
            bookings.append(dict(checkin=checkin, checkout=checkout, payment=payment))

        return bookings

    def generate_persons(self, count: int):
        persons, locations = [], list(DataGenerator.get_locations())
        for _ in range(count):
            fname, lname = self.fake.first_name(), self.fake.last_name()
            gender = random.choice(self.genders)
            email = f"{fname}.{lname}@{self.fake.free_email().split('@')[-1]}".lower()
            dob = self.fake.date_between(start_date="-80y", end_date="-20y")

            location = random.choice(locations)
            persons.append(
                dict(
                    firstname=fname,
                    lastname=lname,
                    gender=gender,
                    email=email,
                    dob=dob,
                    state=location[0],
                    country=location[1],
                )
            )
        return persons


if __name__ == "__main__":
    load_dotenv()
    data_dir = getenv("SEED_DIR")

    users_file = path.join(data_dir, "users.csv")
    guests_file = path.join(data_dir, "guests.csv")
    room_types_file = path.join(data_dir, "room_types.csv")
    addons_file = path.join(data_dir, "addons.csv")
    rooms_file = path.join(data_dir, "rooms.csv")
    bookings_file = path.join(data_dir, "bookings.csv")

    amenities = DataGenerator.to_dataframe(addons)
    room_types = DataGenerator.to_dataframe(room_variations)

    datagen = DataGenerator(getenv("SEED"))

    users = datagen.generate_persons(num_users)
    guests = datagen.generate_persons(num_guests)

    all_guests = DataGenerator.to_dataframe(users + guests)
    all_guests = all_guests.drop_duplicates(subset=["email"])

    users = DataGenerator.to_dataframe(users)
    users = users.drop_duplicates(subset=["email"])
    users = users.drop(columns=["dob"])

    rooms = datagen.generate_rooms(floors, floor_rooms)
    rooms = DataGenerator.to_dataframe(rooms)

    bookings = datagen.generate_bookings(num_bookings, max_stay)
    bookings = DataGenerator.to_dataframe(bookings)
    bookings["idx"] = bookings.index % users.shape[0]
    bookings = bookings.merge(users["email"], left_on="idx", right_index=True)
    bookings = bookings.drop(columns=["idx"])
    bookings = bookings.rename(columns={"email": "user"})

    users.to_csv(users_file, index=False)
    all_guests.to_csv(guests_file, index=False)
    room_types.to_csv(room_types_file, index=False)
    amenities.to_csv(addons_file, index=False)
    rooms.to_csv(rooms_file, index=False)
    bookings.to_csv(bookings_file, index=False)
