INSERT INTO {{ params.mrt_gender }}
WITH max_date AS (
	SELECT MAX(DATE) max_date 
	FROM {{ params.mrt_gender }}
), booking_gender AS (
	SELECT
		d.`date`,
		g.gender,
		SUM(t.price) revenue
	FROM {{ params.fct_bookings }} f
	LEFT JOIN {{ params.dim_date }} d
	ON f.datetime = d.id
	LEFT JOIN {{ params.dim_guest }} g
	ON f.guest = g.id
	LEFT JOIN {{ params.dim_roomtype }} t
	ON f.roomtype = t.id
	WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
	GROUP BY 1, 2
), addon_gender AS (
	SELECT 
		d.`date`,
		g.gender,
		SUM(a.price) revenue
	FROM {{ params.fct_amenities }} f
	LEFT JOIN {{ params.dim_date }} d
	ON f.datetime = d.id
	LEFT JOIN {{ params.dim_guest }} g
	ON f.guest = g.id
	LEFT JOIN {{ params.dim_addon }} a
	ON f.addon = a.id
	WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
	GROUP BY 1, 2
)

SELECT
b.`date`,
b.gender,
b.revenue,
COALESCE(a.revenue, 0) addons_revenue,
b.revenue + COALESCE(a.revenue, 0) total_revenue
FROM booking_gender b
LEFT JOIN addon_gender a
ON b.date = a.date AND b.gender = a.gender;
