from dotenv import load_dotenv
from os import getenv
from common import get_connection_string, cast_datetime
from db_writer import execute_query

load_dotenv()

raw_booking_addon_table = getenv("RAW_BOOKING_ADDON_TABLE")
raw_booking_room_table = getenv("RAW_BOOKING_ROOM_TABLE")
raw_room_table = getenv("RAW_ROOM_TABLE")
raw_guest_table = getenv("RAW_GUEST_TABLE")
dim_roomtype_table = getenv("DIM_ROOMTYPE_TABLE")
dim_guest_table = getenv("DIM_GUEST_TABLE")
dim_location_table = getenv("DIM_LOCATION_TABLE")
dim_addon_table = getenv("DIM_ADDON_TABLE")
fct_amenities_table = getenv("FCT_AMENITIES_TABLE")

query = """
CREATE TEMPORARY TABLE fact_amenities AS
    WITH raw_amenities AS (
        SELECT
            ba.id,
            ba.datetime,
            ba.addon,
            ba.quantity,
            br.guest,
            br.room
        FROM {booking_addons} ba
        INNER JOIN {booking_rooms} br
        ON ba.booking_room = br.id
        WHERE ba.is_deleted = false AND ba.processed = false AND br.is_deleted = false AND TIMESTAMPDIFF(DAY, {datetime}, ba.datetime) < 7
    ), amenities AS (
        SELECT
            a.id,
            a.datetime,
            (
                SELECT MAX(id)
                FROM {dim_guest}
                WHERE _id = a.guest AND created_at <= IF(a.datetime > {datetime}, a.datetime, {datetime})
            ) guest,
            (
                SELECT JSON_OBJECT("state", g.state, "country", g.country)
                FROM {guests} g
                WHERE g.id = a.guest AND g.updated_at <= IF(a.datetime > {datetime}, a.datetime, {datetime})
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) guest_location,
            (
                SELECT type
                FROM {rooms} r
                WHERE r.id = a.room AND r.updated_at <= IF(a.datetime > {datetime}, a.datetime, {datetime})
                ORDER BY r.updated_at DESC
                LIMIT 1
            ) room_type,
            (
                SELECT MAX(id)
                FROM {dim_addon}
                WHERE _id = a.addon AND created_at <= IF(a.datetime > {datetime}, a.datetime, {datetime})
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
                FROM {dim_location}
                WHERE JSON_EXTRACT(a.guest_location, "$.state") = state AND JSON_EXTRACT(a.guest_location, "$.country") = country
            ) guest_location,
            (
                SELECT MAX(id)
                FROM {dim_roomtype}
                WHERE _id = a.room_type AND created_at <= IF(a.datetime > {datetime}, a.datetime, {datetime})
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

    INSERT INTO {fct_amenities} (datetime, guest, guest_location, roomtype, addon, addon_quantity)
    SELECT datetime, guest, guest_location, roomtype, addon, addon_quantity
    FROM fact_amenities;

    UPDATE {booking_addons} ba
    INNER JOIN (
        SELECT id
        FROM fact_amenities
        GROUP BY 1
    ) fa
    ON ba.id = fa.id
    SET ba.processed = true; 
"""


def process_amenities(datetime: str):
    formatted_query = query.format(
        booking_addons=raw_booking_addon_table,
        booking_rooms=raw_booking_room_table,
        dim_guest=dim_guest_table,
        rooms=raw_room_table,
        guests=raw_guest_table,
        dim_location=dim_location_table,
        dim_roomtype=dim_roomtype_table,
        dim_addon=dim_addon_table,
        fct_amenities=fct_amenities_table,
        datetime=cast_datetime(datetime),
    )
    execute_query(get_connection_string(False), formatted_query)
