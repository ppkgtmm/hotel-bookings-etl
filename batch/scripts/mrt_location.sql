INSERT INTO {{ params.mrt_location }}
WITH max_date AS (
	SELECT MAX(DATE) max_date 
	FROM {{ params.mrt_location }}
), booking_location AS (
	SELECT
		d.`date`,
		l.fips,
		l.country,
		l.`state`,
		SUM(t.price) revenue
	FROM {{ params.fct_bookings }} f
	LEFT JOIN {{ params.dim_date }} d
	ON f.datetime = d.id
	LEFT JOIN {{ params.dim_location }} l
	ON f.guest_location = l.id
	LEFT JOIN {{ params.dim_roomtype }} t
	ON f.roomtype = t.id
	WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
	GROUP BY 1, 2, 3, 4
), addon_location AS (
	SELECT
		d.`date`,
		l.fips,
		l.country,
		l.`state`,
		SUM(a.price) revenue
	FROM {{ params.fct_amenities }} f
	LEFT JOIN {{ params.dim_date }} d
	ON f.datetime = d.id
	LEFT JOIN {{ params.dim_location }} l
	ON f.guest_location = l.id
	LEFT JOIN {{ params.dim_addon }} a
	ON f.addon = a.id
	WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
	GROUP BY 1, 2, 3, 4
)
SELECT
b.`date`,
b.fips,
b.state,
b.country,
b.revenue,
COALESCE(a.revenue, 0) addons_revenue,
b.revenue + COALESCE(a.revenue, 0) total_revenue
FROM booking_location b
LEFT JOIN addon_location a
ON b.date = a.date AND b.fips = a.fips;
