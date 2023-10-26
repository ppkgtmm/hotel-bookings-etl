INSERT INTO {{ params.mrt_roomtype }}
WITH max_date AS (
	SELECT MAX(DATE) max_date 
	FROM {{ params.mrt_roomtype }}
)

SELECT
	d.`date`,
	t.`name` room_type,
	COUNT(1) num_booked,
	SUM(t.price) revenue
FROM {{ params.fct_bookings }} f
LEFT JOIN {{ params.dim_date }} d
ON f.datetime = d.id
LEFT JOIN {{ params.dim_roomtype }} t
ON f.roomtype = t.id
WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
GROUP BY 1, 2;
