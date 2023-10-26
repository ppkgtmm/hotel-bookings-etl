INSERT INTO {{ params.mrt_age }}
WITH max_date AS (
	SELECT MAX(DATE) max_date 
	FROM {{ params.mrt_age }}
), raw_booking_age AS (
	SELECT 
		TIMESTAMPDIFF(YEAR, CAST(g.dob AS datetime), CURRENT_TIMESTAMP()) age,
		t.price,
		d.`date`
	FROM {{ params.fct_bookings }} f
	LEFT JOIN {{ params.dim_date }} d
	ON f.datetime = d.id
	LEFT JOIN {{ params.dim_guest }} g
	ON f.guest = g.id
	LEFT JOIN {{ params.dim_roomtype }} t
	ON f.roomtype = t.id
	WHERE (SELECT * FROM max_date) IS NULL OR d.`date` >= (SELECT * FROM max_date)
), raw_addon_age AS (
	SELECT 
		TIMESTAMPDIFF(YEAR, CAST(g.dob AS datetime), CURRENT_TIMESTAMP()) age,
		a.price,
		d.`date`
	FROM {{ params.fct_amenities }} f
	LEFT JOIN {{ params.dim_date }} d
	ON f.datetime = d.id
	LEFT JOIN {{ params.dim_guest }} g
	ON f.guest = g.id
	LEFT JOIN {{ params.dim_addon }} a
	ON f.addon = a.id
	WHERE (SELECT * FROM max_date) IS NULL OR d.`date` >= (SELECT * FROM max_date)
), booking_age AS (
	SELECT
		`date`,
		CASE
		WHEN age < 20 THEN 'less than 20'
		WHEN age BETWEEN 20 AND 29 THEN '20 - 29'
		WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
		WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
		WHEN age BETWEEN 50 AND 59 THEN '50 - 59'
		ELSE 'at least 60'
		END age_range,
		SUM(price) revenue,
		MIN(age) min_age
	FROM raw_booking_age
	GROUP BY 1, 2
), addon_age AS (
	SELECT
		`date`,
		CASE
		WHEN age < 20 THEN 'less than 20'
		WHEN age BETWEEN 20 AND 29 THEN '20 - 29'
		WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
		WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
		WHEN age BETWEEN 50 AND 59 THEN '50 - 59'
		ELSE 'at least 60'
		END age_range,
		SUM(price) revenue
	FROM raw_addon_age
	GROUP BY 1, 2
)

SELECT
b.`date`,
b.age_range,
min_age,
b.revenue,
COALESCE(a.revenue, 0) addons_revenue,
b.revenue + COALESCE(a.revenue, 0) total_revenue
FROM booking_age b
LEFT JOIN addon_age a
ON b.date = a.date AND b.age_range = a.age_range;
