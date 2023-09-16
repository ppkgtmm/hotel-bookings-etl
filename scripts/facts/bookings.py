from dotenv import load_dotenv
from os import getenv
from common import get_connection_string
from db_writer import execute_query

load_dotenv()

raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_booking_table = getenv("RAW_BOOKING_TABLE")

query = """
    WITH RECURSIVE datetimes AS (
        SELECT id booking_id, checkin datetime, checkout
        FROM {bookings}
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
            br.guest,
            br.updated_at
        FROM {booking_rooms} br
        INNER JOIN {bookings} b
        ON br.booking = b.id
        WHERE br.is_deleted = false AND br.processed = false AND b.is_deleted = false AND b.checkin < (CURDATE() - INTERVAL 1 WEEK)
    ), bookings AS (
        SELECT
            b.id,
            b.checkin,
            b.booking,
            (
                SELECT MAX(id)
                FROM {dim_guest}
                WHERE _id = b.guest AND created_at <= b.checkin
            ) guest,
            (
                SELECT JSON_OBJECT("state", g.state, "country", g.country)
                FROM {guests} g
                WHERE g.id = b.guest AND g.updated_at <= b.checkin
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) guest_location,
            (
                SELECT type
                FROM {rooms} r
                WHERE r.id = b.room AND r.updated_at <= b.checkin
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
                WHERE _id = b.room_type AND created_at <= b.checkin
            ) room_type
        FROM bookings b
        WHERE b.guest IS NOT NULL AND b.guest_location IS NOT NULL AND b.room_type IS NOT NULL
    ), fact_bookings AS (
        SELECT 
            b.id,
            DATE_FORMAT(dt.datetime, '%Y%m%d000000') datetime,
            b.guest,
            b.guest_location,
            b.room_type roomtype
        FROM enriched_bookings b
        INNER JOIN datetimes dt
        ON b.booking = dt.booking_id
        WHERE b.guest_location IS NOT NULL AND b.room_type IS NOT NULL
    )
"""
# query = """
#     WITH bookings AS (
#         SELECT
#             br.id,
#             b.checkin,
#             b.checkout,
#             (
#                 SELECT MAX(id)
#                 FROM {dim_guest}
#                 WHERE _id = br.guest AND created_at <= br.updated_at
#             ) guest,
#             (
#                 SELECT type
#                 FROM {rooms} r
#                 WHERE r.id = br.room AND r.updated_at <= br.updated_at
#                 ORDER BY r.updated_at DESC
#                 LIMIT 1
#             ) room_type,
#             (
#                 SELECT JSON_OBJECT("state", g.state, "country", g.country)
#                 FROM {guests} g
#                 WHERE g.id = br.guest AND g.updated_at <= br.updated_at
#                 ORDER BY g.updated_at DESC
#                 LIMIT 1
#             ) guest_location,
#             br.updated_at
#         FROM {booking_rooms} br
#         INNER JOIN {bookings} b
#         ON br.processed = false AND br.booking = b.id
#         WHERE b.checkin < (CURDATE() - INTERVAL 1 WEEK)
#     )
#     SELECT
#         b.id,
#         b.checkin,
#         b.checkout,
#         b.guest,
#         l.id guest_location,
#         (
#             SELECT MAX(id)
#             FROM {dim_roomtype}
#             WHERE _id = b.room_type AND created_at <= b.updated_at

#         ) room_type
#     FROM bookings b
#     INNER JOIN {dim_location} l
#     ON JSON_EXTRACT(b.guest_location, "$.state") = l.state AND JSON_EXTRACT(b.guest_location, "$.country") = l.country
#     WHERE b.room_type IS NOT NULL AND b.guest IS NOT NULL
# """
