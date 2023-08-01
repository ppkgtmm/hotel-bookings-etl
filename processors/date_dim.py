from datetime import timedelta, datetime
from sqlalchemy import create_engine, text
import math
from dotenv import load_dotenv
from os import getenv

load_dotenv()

db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")
connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    db_user, db_password, db_host, db_port, db_name
)


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
            month=curr_date.strftime("%Y-%m-01"),
            quarter=datetime(
                year=curr_date.year,
                month=(math.ceil(curr_date.month / 3) - 1) * 3 + 1,
                day=1,
            ).date(),
            year=curr_date.year,
        )
    )
    curr_date += timedelta(minutes=30)

engine = create_engine(connection_string)
conn = engine.connect()
conn.execute(
    text(
        "INSERT INTO dim_date VALUES (:id, :datetime, :date, DATE(:month), :quarter, :year)"
    ),
    data,
)
conn.commit()
conn.close()
engine.dispose()
