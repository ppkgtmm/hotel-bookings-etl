WITH max_date AS (
	select MAX(date) max_date 
	from mrt_roomtype
)

SELECT
	d.`date`,
	t.`name` room_type,
	COUNT(1) num_booked,
	SUM(t.price) revenue
FROM fct_bookings f
left join dim_date d
on f.datetime = d.id
LEFT JOIN dim_roomtype t
ON f.roomtype = t.id
WHERE (SELECT * from max_date) Is null or d.`date` >= (SELECT * from max_date)
GROUP BY 1, 2;