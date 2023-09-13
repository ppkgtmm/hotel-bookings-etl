import sys
from os.path import abspath
from datetime import date, timedelta
from dotenv import load_dotenv
from os import getenv
import pandas as pd

sys.path.append(abspath("."))
from utilities.db_writer import DatabaseWriter

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")

end_date = date.today() + timedelta(days=1)
start_date = end_date - pd.DateOffset(months=15, days=1)

data = pd.DataFrame({"datetime": pd.date_range(start_date, end_date, freq="H")})
data["id"] = data["datetime"].dt.strftime("%Y%m%d%H%M%S").astype(int)
data["date"] = data["datetime"].dt.date
data["month"] = data["datetime"].dt.to_period("M").dt.to_timestamp().dt.date
data["quarter"] = data["datetime"].dt.to_period("Q").dt.to_timestamp().dt.date
data["year"] = data["datetime"].dt.to_period("Y").dt.to_timestamp().dt.date

db_writer = DatabaseWriter(db_user, db_password, db_host, db_port, db_name)
db_writer.write_dim_date(data.to_dict(orient="records"))
db_writer.tear_down()
