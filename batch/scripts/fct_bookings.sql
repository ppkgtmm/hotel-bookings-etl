CREATE TEMPORARY TABLE fact_bookings AS
    WITH RECURSIVE datetimes AS (
        SELECT id booking_id, checkin datetime, checkout
        FROM {{ params["bookings"] }}
        WHERE is_deleted = false AND (checkin - {{ params["date"] }}) < 7
        UNION ALL
        SELECT booking_id, datetime + INTERVAL 1 DAY, checkout
        FROM datetimes
        WHERE datetime < checkout
    ), raw_bookings AS (
        SELECT 
            br.id,
            b.checkin,
            br.booking,
            br.room,
            br.guest
        FROM {{ params["booking_rooms"] }} br
        INNER JOIN {{ params["bookings"] }} b
        ON br.booking = b.id
        WHERE br.is_deleted = false AND br.processed = false AND b.is_deleted = false AND (b.checkin - {{ params["date"] }}) < 7
    ), bookings AS (
        SELECT
            b.id,
            b.checkin,
            b.booking,
            (
                SELECT MAX(id)
                FROM {{ params["dim_guest"] }}
                WHERE _id = b.guest AND created_at <= IF(b.checkin > {{ params["datetime"] }}, b.checkin, {{ params["datetime"] }})
            ) guest,
            (
                SELECT JSON_OBJECT("state", g.state, "country", g.country)
                FROM {{ params["guests"] }} g
                WHERE g.id = b.guest AND g.updated_at <= IF(b.checkin > {{ params["datetime"] }}, b.checkin, {{ params["datetime"] }})
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) guest_location,
            (
                SELECT type
                FROM {{ params["rooms"] }} r
                WHERE r.id = b.room AND r.updated_at <= IF(b.checkin > {{ params["datetime"] }}, b.checkin, {{ params["datetime"] }})
                ORDER BY r.updated_at DESC
                LIMIT 1
            ) room_type
        FROM raw_bookings b
    ), enriched_bookings AS (
        SELECT
            b.id,
            b.checkin,
            b.booking,
            b.guest,
            (
                SELECT MAX(id)
                FROM {{ params["dim_location"] }}
                WHERE JSON_EXTRACT(b.guest_location, "$.state") = state AND JSON_EXTRACT(b.guest_location, "$.country") = country
            ) guest_location,
            (
                SELECT MAX(id)
                FROM {{ params["dim_roomtype"] }}
                WHERE _id = b.room_type AND created_at <= IF(b.checkin > {{ params["datetime"] }}, b.checkin, {{ params["datetime"] }})
            ) room_type
        FROM bookings b
        WHERE b.guest IS NOT NULL AND b.guest_location IS NOT NULL AND b.room_type IS NOT NULL
    )

    SELECT 
        b.id,
        DATE_FORMAT(dt.datetime, '%Y%m%d000000') datetime,
        b.guest,
        b.guest_location,
        b.room_type roomtype
    FROM enriched_bookings b
    INNER JOIN datetimes dt
    ON b.booking = dt.booking_id
    WHERE b.guest_location IS NOT NULL AND b.room_type IS NOT NULL;
    
    INSERT INTO {{ params["fct_bookings"] }} (datetime, guest, guest_location, roomtype)
    SELECT datetime, guest, guest_location, roomtype
    FROM fact_bookings;

    UPDATE {{ params["booking_rooms"] }} br
    INNER JOIN (
        SELECT id
        FROM fact_bookings
        GROUP BY 1
    ) fb
    ON br.id = fb.id
    SET br.processed = true;