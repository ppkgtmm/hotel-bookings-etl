WITH to_delete AS (
    SELECT booking, SUM(IF(processed = false AND is_deleted = false, 1, 0)) cnt_pending
    FROM {{ params.booking_rooms }}
    GROUP BY booking
)

DELETE b
FROM {{ params.bookings }} b
INNER JOIN to_delete tbd
ON b.id = tbd.booking
WHERE tbd.cnt_pending = 0;
