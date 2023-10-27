from sqlalchemy import create_engine, NullPool, text

summary_by_age = """
SELECT
	age_range,
	COUNT(1) num_days,
	SUM(revenue) revenue,
	SUM(addons_revenue) addons_revenue,
	SUM(total_revenue) total_revenue
FROM mrt_age
WHERE date BETWEEN {start_date} AND {end_date}
GROUP BY 1
ORDER BY MIN(min_age)
"""

summary_by_gender = """
SELECT
	gender,
	COUNT(1) num_days,
	SUM(revenue) revenue,
	SUM(addons_revenue) addons_revenue,
	SUM(total_revenue) total_revenue
FROM mrt_gender
WHERE date BETWEEN {start_date} AND {end_date}
GROUP BY 1
"""

summary_by_location = """
SELECT
	fips,
	state,
	country,
	COUNT(1) num_days,
	SUM(revenue) revenue,
	SUM(addons_revenue) addons_revenue,
	SUM(total_revenue) total_revenue
FROM mrt_location
WHERE date BETWEEN {start_date} AND {end_date}
GROUP BY 1, 2, 3
"""

summary_by_type = """
SELECT
	room_type,
	SUM(num_booked) num_booked,
	SUM(revenue) revenue
FROM mrt_roomtype
WHERE date BETWEEN {start_date} AND {end_date}
GROUP BY 1
"""

summary_by_addon = """
SELECT
	addon,
	SUM(quantity) quantity,
	SUM(revenue) revenue
FROM mrt_addon
WHERE date BETWEEN {start_date} AND {end_date}
GROUP BY 1
"""


def fetch_data(connection_string: str, query: str):
    engine = create_engine(connection_string, poolclass=NullPool)
    with engine.connect() as conn:
        rows = conn.execute(text(query))
        return [row._asdict() for row in rows]
