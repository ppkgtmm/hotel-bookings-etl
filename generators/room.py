import pandas as pd
from faker.generator import random
from dotenv import load_dotenv
from os import getenv

load_dotenv()

floors = 30
floor_rooms = 40
data_dir = getenv("SEED_DIR")
seed = getenv("SEED")

random.seed(seed)
room_types_file = data_dir + "room_types.csv"

room_types = pd.read_csv(room_types_file)
room_types.sort_values(by="price", ascending=False, inplace=True)
room_types.reset_index(inplace=True, drop=True)
room_types.reset_index(inplace=True, names=["index"])

indices = list(room_types.index + 1)
weights = [i / sum(indices) for i in indices]


def generate_rooms(floors: int, rooms: int):
    result = []
    for floor in range(1, floors + 1):
        for room_number in range(1, rooms + 1):
            result.append(dict(floor=floor, number=room_number))
    return result


if __name__ == "__main__":
    rooms = pd.DataFrame(generate_rooms(floors, floor_rooms))
    types = (
        random.choices(room_types.name.tolist(), weights=weights, k=floor_rooms)
        * floors
    )
    rooms["type"] = pd.Series(types)
    rooms.to_csv(data_dir + "rooms.csv", index=False)
