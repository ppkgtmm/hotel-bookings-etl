INSERT INTO {{ params.full_picture }}
WITH max_date AS (
	SELECT MAX(`date`) max_date 
	FROM {{ params.full_picture }}
), bookings AS (
    SELECT d.`date`, f.guest, g.dob guest_dob, TIMESTAMPDIFF(YEAR, CAST(g.dob AS datetime), CURRENT_TIMESTAMP()) guest_age, get_age_range(TIMESTAMPDIFF(YEAR, CAST(g.dob AS datetime), CURRENT_TIMESTAMP())) age_range, l.country guest_country, t.name room_type, t.price
    FROM {{ params.fct_bookings }} f
    LEFT JOIN {{ params.dim_date }} d
    ON f.datetime = d.id
    LEFT JOIN {{ params.dim_guest }} g
    ON f.guest = g.id
    LEFT JOIN {{ params.dim_location }} l
    ON f.guest_location = l.id
    LEFT JOIN {{ params.dim_roomtype }} t
    ON f.roomtype = t.id
    WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
), amenities AS (
    SELECT d.`date`, f.guest, JSON_ARRAYAGG(JSON_OBJECT("addon", a.name, "price", a.price, "quantity", f.addon_quantity)) addons
    FROM {{ params.fct_amenities }} f
    LEFT JOIN {{ params.dim_date }} d
    ON f.datetime = d.id
    LEFT JOIN {{ params.dim_addon }} a
    ON f.addon = a.id
    WHERE (SELECT * FROM max_date) IS NULL OR d.`date` > (SELECT * FROM max_date)
    GROUP BY 1, 2

)

SELECT b.*, a.addons
FROM bookings b
LEFT JOIN amenities a
ON b.date = a.date AND b.guest = a.guest
