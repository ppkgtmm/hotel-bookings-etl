from datetime import date, timedelta
from dotenv import load_dotenv
from os import getenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")
dim_date_table = getenv("DIM_DATE_TABLE")
fmt = getenv("DT_FORMAT")


connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

end_date = date.today() + timedelta(days=1)
start_date = end_date - pd.DateOffset(months=15, days=1)

data = pd.DataFrame({"datetime": pd.date_range(start_date, end_date, freq="H")})
data["id"] = data["datetime"].dt.strftime(fmt).astype(int)
data["date"] = data["datetime"].dt.date
data["month"] = data["datetime"].dt.to_period("M").dt.to_timestamp().dt.date
data["quarter"] = data["datetime"].dt.to_period("Q").dt.to_timestamp().dt.date
data["year"] = data["datetime"].dt.to_period("Y").dt.to_timestamp().dt.date

engine = create_engine(connection_string)

with engine.connect() as conn:
    data.to_sql(dim_date_table, conn, index=False, if_exists="append")

engine.dispose()
