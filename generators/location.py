import pandas as pd
from dotenv import load_dotenv
from os import getenv

load_dotenv()

state_file_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/states.csv"
data_dir = getenv("SEED_DIR")

if __name__ == "__main__":
    states = pd.read_csv(state_file_url)[["name", "country_name"]]
    states.columns = ["state", "country"]
    states = states.drop_duplicates()
    states.to_csv(data_dir + "location.csv", index=False)
