WITH max_date AS (
	select MAX(date) max_date 
	from mrt_addon
)

SELECT
	d.`date`,
	a.`name` addon,
	SUM(f.addon_quantity) quantity,
	SUM(a.price) revenue
FROM fct_amenities f
left join dim_date d
on f.datetime = d.id
LEFT JOIN dim_addon a
ON f.addon = a.id
WHERE (SELECT * from max_date) Is null or d.`date` >= (SELECT * from max_date)
GROUP BY 1, 2;
