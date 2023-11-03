from dotenv import load_dotenv
from os import getenv, path
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

data_file = "full_picture.csv"
geo_json_file = "states_provinces.json"
query_file_path = path.abspath(path.join(path.dirname(__file__), "full_picture.sql"))
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

    with open(query_file_path, "r") as fp:
        query = fp.read()

    dashboard_data = pd.read_sql(query, conn)
    dashboard_data.to_csv(path.join(output_folder, data_file), index=False)

    conn.close()
    engine.dispose()
