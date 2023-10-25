from dotenv import load_dotenv
from os import getenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

file_url = getenv("LOCATION_FILE")
db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")
dim_location_table = getenv("DIM_LOCATION_TABLE")

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)


data = pd.read_csv(file_url)
data = data.rename(columns={"name": "state", "admin": "country"})

engine = create_engine(connection_string)

with engine.connect() as conn:
    data.to_sql(dim_location_table, conn, index=False, if_exists="append")

engine.dispose()
