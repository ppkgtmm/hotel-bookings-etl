from dotenv import load_dotenv
from os import getenv
from common import get_connection_string
from db_writer import execute_query

load_dotenv()

raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")
raw_room_table = getenv("RAW_ROOM_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")
dim_guest_table = getenv("DIM_GUEST_TABLE")
dim_location_table = getenv("DIM_LOCATION_TABLE")
fct_booking_table = getenv("FCT_BOOKING_TABLE")

query = """
CREATE TEMPORARY TABLE fact_bookings AS
    WITH RECURSIVE datetimes AS (
        SELECT id booking_id, checkin datetime, checkout
        FROM {bookings}
        WHERE is_deleted = false AND (checkin - CURDATE()) < 7
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
        FROM {booking_rooms} br
        INNER JOIN {bookings} b
        ON br.booking = b.id
        WHERE br.is_deleted = false AND br.processed = false AND b.is_deleted = false AND (b.checkin - CURDATE()) < 7
    ), bookings AS (
        SELECT
            b.id,
            b.checkin,
            b.booking,
            (
                SELECT MAX(id)
                FROM {dim_guest}
                WHERE _id = b.guest AND created_at <= IF(b.checkin > CURRENT_TIMESTAMP(), b.checkin, CURRENT_TIMESTAMP())
            ) guest,
            (
                SELECT JSON_OBJECT("state", g.state, "country", g.country)
                FROM {guests} g
                WHERE g.id = b.guest AND g.updated_at <= IF(b.checkin > CURRENT_TIMESTAMP(), b.checkin, CURRENT_TIMESTAMP())
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) guest_location,
            (
                SELECT type
                FROM {rooms} r
                WHERE r.id = b.room AND r.updated_at <= IF(b.checkin > CURRENT_TIMESTAMP(), b.checkin, CURRENT_TIMESTAMP())
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
                FROM {dim_location}
                WHERE JSON_EXTRACT(b.guest_location, "$.state") = state AND JSON_EXTRACT(b.guest_location, "$.country") = country
            ) guest_location,
            (
                SELECT MAX(id)
                FROM {dim_roomtype}
                WHERE _id = b.room_type AND created_at <= IF(b.checkin > CURRENT_TIMESTAMP(), b.checkin, CURRENT_TIMESTAMP())
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
    
    INSERT INTO {fct_bookings} (datetime, guest, guest_location, roomtype)
    SELECT datetime, guest, guest_location, roomtype
    FROM fact_bookings;

    UPDATE {booking_rooms} br
    INNER JOIN (
        SELECT id
        FROM fact_bookings
        GROUP BY 1
    ) fb
    ON br.id = fb.id
    SET br.processed = true;
"""


def process_fct_bookings():
    print(
        query.format(
            bookings=raw_booking_table,
            booking_rooms=raw_booking_room_table,
            dim_guest=dim_guest_table,
            rooms=raw_room_table,
            guests=raw_guest_table,
            dim_location=dim_location_table,
            dim_roomtype=dim_roomtype_table,
            fct_bookings=fct_booking_table,
        )
    )


process_fct_bookings()
