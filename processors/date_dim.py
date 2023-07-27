from datetime import timedelta, datetime
import pandas as pd
from helpers import Processor

df_columns = ["id", "datetime", "date", "month", "quarter", "year"]
start_date = pd.to_datetime("2021-01-01") + timedelta(days=-90)
curr_date = start_date
max_date = datetime.now()
data = []
while curr_date <= max_date:
    data.append(
        (
            curr_date.strftime("%Y%m%d%H%M%S"),
            curr_date,
            curr_date.date(),
            curr_date.strftime("%Y-%m-01"),
            datetime(year=curr_date.year, month=(curr_date.quarter - 1) * 3 + 1, day=1),
            curr_date.year,
        )
    )
    curr_date += timedelta(minutes=30)

Processor.load_envars()
Processor.setup_db_conn()

pd.DataFrame(data, columns=df_columns).to_sql("dim_date", Processor.conn)

Processor.teardown_db_conn()
