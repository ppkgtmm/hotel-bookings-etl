stg_location_table = "stg_location"
stg_room_table = "stg_room"
stg_guest_table = "stg_guest"
stg_booking_table = "stg_booking"
stg_booking_room_table = "stg_booking_room"
stg_booking_addon_table = "stg_booking_addon"

dim_date_table = "dim_date"
dim_roomtype_table = "dim_roomtype"
dim_addon_table = "dim_addon"
dim_guest_table = "dim_guest"
dim_location_table = "dim_location"
fct_booking_table = "fct_booking"
fct_purchase_table = "fct_purchase"

bookings_query = """
    WITH bookings AS (
        SELECT
            br.id,
            b.checkin, 
            b.checkout,
            br.guest,
            br.updated_at,
            (
                SELECT location
                FROM {stg_guest_table} g
                WHERE g.id = br.guest AND g.updated_at <= br.updated_at
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) guest_location,
            (
                SELECT type
                FROM {stg_room_table} r
                WHERE r.id = br.room AND r.updated_at <= br.updated_at
                ORDER BY r.updated_at DESC
                LIMIT 1
            ) room_type
        FROM {stg_booking_room_table} br
        INNER JOIN {stg_booking_table} b
        ON br.processed = false AND br.booking = b.id
    )
    SELECT
        b.id,
        b.checkin, 
        b.checkout,
        b.guest,
        b.guest_location,
        (
            SELECT MAX(id)
            FROM dim_roomtype
            WHERE _id = b.room_type AND created_at <= b.updated_at

        ) room_type
    FROM bookings b
    INNER JOIN dim_location l
    ON b.guest_location = l.id
    WHERE b.room_type IS NOT NULL
"""