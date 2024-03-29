WITH to_delete AS (
    SELECT booking_room, SUM(IF(processed = false AND is_deleted = false, 1, 0)) cnt_pending
    FROM {{ params.booking_addons }}
    GROUP BY booking_room
)

DELETE br
FROM {{ params.booking_rooms }} br
LEFT JOIN to_delete tbd
ON br.id = tbd.booking_room
WHERE tbd.booking_room IS NULL OR br.is_deleted = true OR tbd.cnt_pending = 0;
