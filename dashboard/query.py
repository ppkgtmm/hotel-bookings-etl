from sqlalchemy import create_engine, NullPool, text

summary_by_age = """
WITH cte AS (
	SELECT 
		TIMESTAMPDIFF(YEAR, CAST(g.dob AS datetime), CURRENT_TIMESTAMP()) age,
		t.price
	FROM fct_bookings b
	LEFT JOIN dim_guest g
	ON b.guest = g.id
	LEFT JOIN dim_roomtype t
	ON b.roomtype = t.id
	WHERE b.datetime BETWEEN {start_datetime} AND {end_datetime}
)

SELECT 
	CASE
	WHEN age < 20 THEN 'less than 20'
	WHEN age BETWEEN 20 AND 29 THEN '20 - 29'
	WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
	WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
	WHEN age BETWEEN 50 AND 59 THEN '50 - 59'
	ELSE 'at least 60'
	END age_range,
	COUNT(1) num_days,
	SUM(price) revenue
FROM cte
GROUP BY 1
ORDER BY MIN(age);
"""

summary_by_gender = """
SELECT 
	g.gender,
	COUNT(1) num_days,
	SUM(price) revenue
FROM fct_bookings b
LEFT JOIN dim_guest g
ON b.guest = g.id
LEFT JOIN dim_roomtype t
ON b.roomtype = t.id
WHERE b.datetime BETWEEN {start_datetime} AND {end_datetime}
GROUP BY 1;
"""

summary_by_location = """
SELECT 
	l.state, 
	l.country,
	COUNT(1) num_days,
	SUM(t.price) revenue
FROM fct_bookings b
LEFT JOIN dim_location l
ON b.guest_location = l.id
LEFT JOIN dim_roomtype t
ON b.roomtype = t.id
WHERE b.datetime BETWEEN {start_datetime} AND {end_datetime}
GROUP BY 1, 2;
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
