from dotenv import load_dotenv
from os import getenv, path
from sqlalchemy import create_engine
import pandas as pd
import requests

load_dotenv()

query_files = ["bookings", "addons"]
query_folder = path.abspath(path.join(path.dirname(__file__), "queries"))
output_folder = path.abspath(path.join(path.dirname(__file__), "data"))

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("DWH_DB")
geo_json_url = getenv("GEO_JSON_FILE")

connection_str = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

if __name__ == "__main__":
    engine = create_engine(connection_str)
    conn = engine.connect()

    for file in query_files:
        with open(path.join(query_folder, file + ".sql"), "r") as fp:
            query = fp.read()
        data = pd.read_sql(query, conn)
        data.to_csv(path.join(output_folder, file + ".csv"), index=False)

    conn.close()
    engine.dispose()
