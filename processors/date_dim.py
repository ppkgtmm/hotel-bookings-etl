from datetime import timedelta, datetime
import math


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


# conn.execute(
#     text(
#         "INSERT INTO dim_date VALUES (:id, :datetime, :date, DATE(:month), :quarter, :year)"
#     ),
#     data,
# )
# conn.commit()
# conn.close()
# engine.dispose()
