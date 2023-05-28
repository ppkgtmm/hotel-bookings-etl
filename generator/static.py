import pandas as pd

data_dir = "data/static/"

room_types = [
    dict(name="standard", price=1500),
    dict(name="superior", price=2200),
    dict(name="deluxe", price=2900),
    dict(name="suite", price=3600),
]
addons = [
    dict(name="extra bed", price=750),
    dict(name="baby cot", price=400),
    dict(name="wheelchair", price=150),
    dict(name="wine", price=1000),
    dict(name="breakfast", price=400),
    dict(name="dry cleaning", price=180),
    dict(name="parking space", price=500),
]

if __name__ == "__main__":
    roomtypes_df = pd.DataFrame(room_types)
    addons_df = pd.DataFrame(addons)
    roomtypes_df.to_csv(data_dir + "room_types.csv", index=False)
    addons_df.to_csv(data_dir + "addons.csv", index=False)
