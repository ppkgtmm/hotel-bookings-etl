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
	l.state, 
	l.country,
	l.fips,
	COUNT(1) num_days,
	SUM(t.price) revenue
FROM fct_bookings b
LEFT JOIN dim_location l
ON b.guest_location = l.id
LEFT JOIN dim_roomtype t
ON b.roomtype = t.id
WHERE b.datetime BETWEEN {start_datetime} AND {end_datetime}
GROUP BY 1, 2, 3;
"""

summary_by_type = """
SELECT 
	t.name,
	COUNT(1) num_days,
	SUM(t.price) revenue
FROM fct_bookings b
LEFT JOIN dim_roomtype t
ON b.roomtype = t.id
WHERE b.datetime BETWEEN {start_datetime} AND {end_datetime}
GROUP BY 1;
"""


def fetch_data(connection_string: str, query: str):
    engine = create_engine(connection_string, poolclass=NullPool)
    with engine.connect() as conn:
        rows = conn.execute(text(query))
        return [row._asdict() for row in rows]
