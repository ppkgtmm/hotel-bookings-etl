import sys
from os.path import abspath
from datetime import timedelta, datetime
import math
from dotenv import load_dotenv
from os import getenv

sys.path.append(abspath("./"))
from utilities.db_writer import DatabaseWriter

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")

start_date = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S") + timedelta(
    days=-90
)
curr_date = start_date
max_date = datetime.now()
data = []
while curr_date <= max_date:
    data.append(
        dict(
            id=int(curr_date.strftime("%Y%m%d%H%M%S")),
            datetime=curr_date,
            date=curr_date.date(),
            month=datetime(year=curr_date.year, month=curr_date.month, day=1),
            quarter=datetime(
                year=curr_date.year,
                month=(math.ceil(curr_date.month / 3) - 1) * 3 + 1,
                day=1,
            ).date(),
            year=curr_date.year,
        )
    )
    curr_date += timedelta(minutes=30)

db_writer = DatabaseWriter(db_user, db_password, db_host, db_port, db_name)
db_writer.write_dim_date(data)
db_writer.tear_down()
