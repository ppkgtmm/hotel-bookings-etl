stg_room_table = "stg_room"
stg_guest_table = "stg_guest"
stg_booking_table = "stg_booking"
del_booking_table = "del_booking"
stg_booking_room_table = "stg_booking_room"
del_booking_room_table = "del_booking_room"
stg_booking_addon_table = "stg_booking_addon"
del_booking_addon_table = "del_booking_addon"

dim_date_table = "dim_date"
dim_roomtype_table = "dim_roomtype"
dim_addon_table = "dim_addon"
dim_guest_table = "dim_guest"
dim_location_table = "dim_location"
fct_booking_table = "fct_booking"
fct_purchase_table = "fct_amenities"

location_query = "SELECT id, state, country FROM {}"
guest_query = "SELECT id, email, dob, gender, location, updated_at FROM {}"
addon_query = "SELECT id, name, price, updated_at FROM {}"
roomtype_query = "SELECT id, name, price, updated_at FROM {}"
room_query = "SELECT id, type, updated_at FROM {}"
booking_query = "SELECT id, checkin, checkout FROM {}"
booking_room_query = "SELECT id, booking, room, guest, updated_at FROM {}"
booking_addon_query = (
    "SELECT id, booking_room, addon, quantity, datetime, updated_at FROM {}"
)

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
            FROM {dim_roomtype_table}
            WHERE _id = b.room_type AND created_at <= b.updated_at

        ) room_type
    FROM bookings b
    INNER JOIN {dim_location_table} l
    ON b.guest_location = l.id
    WHERE b.room_type IS NOT NULL
"""

remove_bookings_query = """
    SELECT 
        br.id,
        db.checkin,
        db.checkout,
        br.guest
    FROM {del_booking_room_table} br
    INNER JOIN {del_booking_table} db
    ON br.processed = false AND br.booking = db.id
"""

purchases_query = """
    with purchases AS (
        SELECT
            ba.id,
            ba.datetime,
            br.guest,
            (
                SELECT location
                FROM {stg_guest_table} g
                WHERE g.id = br.guest AND g.updated_at <= ba.updated_at
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) guest_location,
            (
                SELECT type
                FROM {stg_room_table} r
                WHERE r.id = br.room AND r.updated_at <= ba.updated_at
                ORDER BY r.updated_at DESC
                LIMIT 1
            ) room_type,
            (
                SELECT MAX(id)
                FROM {dim_addon_table}
                WHERE _id = ba.addon AND created_at <= ba.updated_at
            ) addon,
            ba.quantity,
            ba.updated_at
        FROM {stg_booking_addon_table} ba
        INNER JOIN {stg_booking_room_table} br
        ON ba.processed = false AND ba.booking_room = br.id
    )
    SELECT
        p.id,
        p.datetime,
        p.guest,
        p.guest_location,
        (
            SELECT MAX(id)
            FROM {dim_roomtype_table}
            WHERE _id = p.room_type AND created_at <= p.updated_at
        ) room_type,
        p.addon,
        p.quantity
    FROM purchases p
    INNER JOIN {dim_location_table} l
    ON p.guest_location = l.id
    WHERE p.room_type IS NOT NULL AND p.addon IS NOT NULL
"""

remove_purchases_query = """
        SELECT
            ba.id,
            ba.datetime,
            dbr.guest,
            (
                SELECT MAX(id)
                FROM {dim_addon_table}
                WHERE _id = ba.addon AND created_at <= ba.updated_at
            ) addon
        FROM {del_booking_addon_table} ba
        INNER JOIN {del_booking_room_table} dbr
        ON ba.processed = false AND ba.booking_room = dbr.id
"""

delete_stg_booking_addons = """
    DELETE
    FROM {stg_booking_addon_table}
    WHERE datetime <= CAST('{datetime}' AS DATETIME) AND processed = true
"""

delete_del_booking_addons = """
    DELETE
    FROM {del_booking_addon_table}
    WHERE datetime <= CAST('{datetime}' AS DATETIME) AND processed = true
"""

delete_stg_booking_rooms = """
    DELETE
    FROM {stg_booking_room_table}
    WHERE processed = true AND booking IN (
        SELECT id
        FROM {stg_booking_table}
        WHERE checkout <= DATE('{date}')
        UNION
        SELECT id
        FROM {del_booking_table}
        WHERE checkout <= DATE('{date}')
    )
"""

delete_del_booking_rooms = """
    DELETE
    FROM {del_booking_room_table}
    WHERE processed = true AND booking IN (
        SELECT id
        FROM {stg_booking_table}
        WHERE checkout <= DATE('{date}')
        UNION
        SELECT id
        FROM {del_booking_table}
        WHERE checkout <= DATE('{date}')
    )
"""

delete_stg_bookings = """
    DELETE sb
    FROM {stg_booking_table} sb
    LEFT JOIN (
        SELECT booking
        FROM {stg_booking_room_table}
        WHERE processed = false
        UNION
        SELECT booking
        FROM {del_booking_room_table}
        WHERE processed = false
    ) br
    ON br.booking = sb.id
    WHERE br.booking IS NULL AND checkout <= DATE('{date}')
"""

delete_del_bookings = """
    DELETE db
    FROM {del_booking_table} db
    LEFT JOIN (
        SELECT booking
        FROM {stg_booking_room_table}
        WHERE processed = false
        UNION
        SELECT booking
        FROM {del_booking_room_table}
        WHERE processed = false
    ) br
    ON br.booking = db.id
    WHERE br.booking IS NULL AND checkout <= DATE('{date}')
"""

delete_rooms_query = """
    DELETE stg
    FROM {stg_room_table} stg
    INNER JOIN (
        SELECT id, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnum
        FROM {stg_room_table}
    ) sub
    ON sub.rnum > 3 AND stg.id = sub.id
"""

delete_guests_query = """
    DELETE stg
    FROM {stg_guest_table} stg
    INNER JOIN (
        SELECT id, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnum
        FROM {stg_guest_table}
    ) sub
    ON sub.rnum > 3 AND stg.id = sub.id
"""
