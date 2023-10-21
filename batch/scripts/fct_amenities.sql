CREATE TEMPORARY TABLE fact_amenities AS
    WITH raw_amenities AS (
        SELECT
            ba.id,
            ba.datetime,
            ba.addon,
            ba.quantity,
            br.guest,
            br.room
        FROM {{ params.booking_addons }} ba
        INNER JOIN {{ params.booking_rooms }} br
        ON ba.booking_room = br.id
        WHERE ba.is_deleted = false AND ba.processed = false AND br.is_deleted = false AND TIMESTAMPDIFF(DAY, CAST('{{ macros.datetime.fromisoformat(ts) + macros.timedelta(days=1) }}' AS DATETIME), ba.datetime) < 7
    ), amenities AS (
        SELECT
            a.id,
            a.datetime,
            (
                SELECT MAX(id)
                FROM {{ params.dim_guest }}
                WHERE _id = a.guest
            ) guest,
            (
                SELECT JSON_OBJECT("state", g.state, "country", g.country)
                FROM {{ params.guests }} g
                WHERE g.id = a.guest
            ) guest_location,
            (
                SELECT type
                FROM {{ params.rooms }} r
                WHERE r.id = a.room
            ) room_type,
            (
                SELECT MAX(id)
                FROM {{ params.dim_addon }}
                WHERE _id = a.addon
            ) addon,
            a.quantity
        FROM raw_amenities a
    ), enriched_amenities AS (
        SELECT
            a.id,
            a.datetime,
            a.guest,
            a.addon,
            a.quantity,
            (
                SELECT MAX(id)
                FROM {{ params.dim_location }}
                WHERE JSON_EXTRACT(a.guest_location, "$.state") = state AND JSON_EXTRACT(a.guest_location, "$.country") = country
            ) guest_location,
            (
                SELECT MAX(id)
                FROM {{ params.dim_roomtype }}
                WHERE _id = a.room_type
            ) room_type
        FROM amenities a
        WHERE a.guest IS NOT NULL AND a.guest_location IS NOT NULL AND a.room_type IS NOT NULL AND a.addon IS NOT NULL
    )
SELECT
    id,
    DATE_FORMAT(datetime, '%Y%m%d%H%i%s') datetime,
    guest,
    guest_location,
    room_type roomtype,
    addon,
    quantity addon_quantity
FROM enriched_amenities
WHERE guest_location IS NOT NULL AND room_type IS NOT NULL;

INSERT INTO {{ params.fct_amenities }} (datetime, guest, guest_location, roomtype, addon, addon_quantity)
SELECT datetime, guest, guest_location, roomtype, addon, addon_quantity
FROM fact_amenities;

UPDATE {{ params.booking_addons }} ba
INNER JOIN (
    SELECT id
    FROM fact_amenities
    GROUP BY 1
) fa
ON ba.id = fa.id
SET ba.processed = true;
