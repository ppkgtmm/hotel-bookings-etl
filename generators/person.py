from faker import Faker
from faker.generator import random
import pandas as pd
from dotenv import load_dotenv
from os import getenv

load_dotenv()

state_file_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/states.csv"
sep = ">>>"
genders = ["M", "F", "Prefer not to say"]

data_dir = getenv("SEED_DIR")
seed = getenv("SEED")

fake = Faker(locale="en_US")
Faker.seed(seed)


states = pd.read_csv(state_file_url)[["name", "country_name"]]
states["state"] = states["name"] + sep + states["country_name"]
states = states["state"].unique().tolist()


def generate_person(count: int):
    result = []
    for _ in range(count):
        fname = fake.first_name()
        lname = fake.last_name()
        gender = fake.random.choice(genders)
        email = f"{fname}.{lname}@{fake.free_email().split('@')[-1]}".lower()
        dob = fake.date_between(start_date="-80y", end_date="-20y")
        state = random.choice(states)
        state_name, country = state.split(sep)[0], state.split(sep)[-1]
        result.append(
            dict(
                firstname=fname,
                lastname=lname,
                gender=gender,
                email=email,
                dob=dob,
                state=state_name,
                country=country,
            )
        )
    return result


if __name__ == "__main__":
    num_users = 30
    num_guests = 50
    users = pd.DataFrame(generate_person(num_users))
    guests = pd.DataFrame(generate_person(num_guests))
    guests = pd.concat([guests, users]).reset_index(drop=True)
    users.drop(columns=["dob"], inplace=True)
    users.drop_duplicates(subset=["email"], inplace=True)
    guests.drop_duplicates(subset=["email"], inplace=True)
    users.to_csv(data_dir + "users.csv", index=False)
    guests.to_csv(data_dir + "guests.csv", index=False)
