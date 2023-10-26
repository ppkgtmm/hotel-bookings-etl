INSERT INTO {{ params.mrt_addon }}
WITH max_date AS (
	SELECT MAX(DATE) max_date 
	FROM {{ params.mrt_addon }}
)

SELECT
	d.`date`,
	a.`name` addon,
	SUM(f.addon_quantity) quantity,
	SUM(a.price) revenue
FROM {{ params.fct_amenities }} f
LEFT JOIN {{ params.dim_date }} d
ON f.datetime = d.id
LEFT JOIN {{ params.dim_addon }} a
ON f.addon = a.id
WHERE (SELECT * FROM max_date) IS NULL OR d.`date` >= (SELECT * FROM max_date)
GROUP BY 1, 2;
