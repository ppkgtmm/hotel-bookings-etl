SET 'auto.offset.reset' = 'earliest';
SET 'cache.max.bytes.buffering' = '0';

CREATE STREAM bookings WITH (
    kafka_topic = 'bookings',
    value_format = 'avro'
);

CREATE STREAM booking_rooms WITH (
    kafka_topic = 'booking_rooms',
    value_format = 'avro'
);

CREATE STREAM bookings_before_repart WITH (kafka_topic='bookings_before_repart', key_format='avro', value_format='avro') AS
SELECT
    before->id,
    before->checkin,
    before->checkout,
    before->updated_at
FROM bookings
WHERE before IS NOT NULL
PARTITION BY before->id;

CREATE STREAM bookings_before_repart WITH (kafka_topic='booking_rooms_before_repart', key_format='avro', value_format='avro') AS
SELECT
    before->booking,
    before->id,
    before->room,
    before->guest,
    before->updated_at
FROM booking_rooms
WHERE before IS NOT NULL
PARTITION BY before->booking;

-- CREATE STREAM deleted_fct_bookings (
--     table_key STRUCT<key1 INT, key2 BIGINT> KEY,
--     id INT,
--     booking INT,
--     room INT,
--     guest INT,
--     checkin INT,
--     checkout INT,
--     booking_room_updated BIGINT,
--     booking_updated BIGINT
-- ) WITH (kafka_topic='deleted_fct_bookings', key_format='avro', value_format='avro', partitions=1);

-- INSERT INTO deleted_fct_bookings
-- STRUCT(key1 := br.before->id, key2 := br.before->updated_at) table_key,
CREATE STREAM bookings_deleted WITH (kafka_topic='bookings_deleted', key_format='avro', value_format='avro') AS
SELECT
    br.booking,
    br.id,
    br.room,
    br.guest,
    b.checkin,
    b.checkout,
    br.updated_at booking_room_updated,
    b.updated_at booking_updated
FROM booking_rooms_before_repart br
LEFT JOIN bookings_before_repart b
WITHIN 5 MINUTES GRACE PERIOD 5 MINUTES
ON br.booking = b.id
EMIT CHANGES;

-- SELECT br_id, count(1) cnt
-- FROM bookings_deleted 
-- WHERE booking_room_updated >= booking_updated
-- GROUP BY br_id
-- EMIT CHANGES;

-- CREATE TABLE diff_table AS
-- SELECT 
--     br_id, 
--     booking,
--     room,
--     guest,
--     booking_room_updated,
--     checkin,
--     checkout,
--     MIN((booking_room_updated - booking_updated)) min_diff
-- FROM bookings_deleted 
-- WHERE booking_room_updated >= booking_updated
-- GROUP BY br_id, booking_room_updated, checkin, checkout
-- EMIT CHANGES;

-- CREATE STREAM diff_stream WITH(kafka_topic = 'DIFF_TABLE', key_format='avro', value_format='avro');

-- SELECT 
--     bd.br_id,
--     bd.booking,
--     bd.room,
--     bd.guest,
--     df.checkin,
--     df.checkout
-- FROM bookings_deleted bd
-- LEFT JOIN diff_stream df
-- ON bd.br_id = df.br_id AND bd.booking_room_updated = df.booking_room_updated 
-- AND bd.checkin = df.checkin AND bd.checkout = df.checkout
-- AND (bd.booking_room_updated - bd.booking_updated) = df.min_diff;

    -- (
    --     SELECT checkin
    --     FROM bookings_deleted 
    --     WHERE bd.id = id AND bd.booking_room_updated = booking_room_updated
    --     AND bd.booking_room_updated >= booking_updated
    --     ORDER BY (booking_room_updated - booking_updated)
    --     LIMIT 1
    -- ) rel_checkin
--     -- b.checkin,
    -- b.checkout,
    -- br.updated_at booking_room_updated,
    -- b.updated_at booking_updated

-- LEFT JOIN (
--     SELECT *
--     FROM bookings_deleted 
--     WHERE booking_room_updated >= booking_updated
--     ORDER BY (booking_room_updated - booking_updated)
-- ) sub
-- ON bd.br_id = sub.br_id
-- WHERE cnt = 1

-- CREATE STREAM deleted_bookings AS 
-- SELECT 
--     before->id,
--     before->checkin,
--     before->checkout,
--     FROM_UNIXTIME(before->updated_at) booking_updated_at
-- FROM bookings
-- WHERE before IS NOT NULL;

-- CREATE STREAM deleted_booking_rooms AS 
-- SELECT 
--     before->id,
--     before->booking,
--     before->room,
--     before->guest,
--     FROM_UNIXTIME(before->updated_at) booking_room_updated_at
-- FROM booking_rooms
-- WHERE before IS NOT NULL;

-- CREATE TABLE earliest_deleted_bookings AS 
-- SELECT 
--     id key,
--     AS_VALUE(id) id,
--     MIN(booking_updated_at) earliest_booking_updated_at
-- FROM deleted_bookings
-- WINDOW TUMBLING (SIZE 5 MINUTES)
-- GROUP BY id;

-- CREATE TABLE earliest_deleted_booking_rooms AS 
-- SELECT 
--     id key,
--     AS_VALUE(id) id,
--     MIN(booking_room_updated_at) earliest_booking_room_updated_at
-- FROM deleted_booking_rooms
-- WINDOW TUMBLING (SIZE 5 MINUTES)
-- GROUP BY id;

-- CREATE STREAM earliest_deleted_bookings_stream (id INT, earliest_booking_updated_at TIMESTAMP)
-- WITH (
--     KAFKA_TOPIC = 'EARLIEST_DELETED_BOOKINGS',
--     PARTITIONS = 1,
--     FORMAT = 'JSON'
-- );

-- CREATE STREAM earliest_deleted_booking_rooms_stream (id INT, earliest_booking_room_updated_at TIMESTAMP)
-- WITH (
--     KAFKA_TOPIC = 'EARLIEST_DELETED_BOOKING_ROOMS',
--     PARTITIONS = 1,
--     FORMAT = 'JSON'
-- );

-- SELECT 
--     br.booking,
--     br.room,
--     br.guest,
--     br.booking_room_updated_at,
--     b.checkin,
--     b.checkout,
--     b.booking_updated_at
-- SELECT br.*
-- FROM deleted_booking_rooms br
-- INNER JOIN EARLIEST_DELETED_BOOKING_ROOMS ebr
-- WITHIN 5 MINUTES GRACE PERIOD 5 MINUTES
-- ON br.id = ebr.id
-- WHERE br.booking_room_updated_at = ebr.earliest_booking_room_updated_at;
-- LEFT JOIN earliest_deleted_bookings b
-- ON br.booking = b.id;

-- WITHIN 5 MINUTES GRACE PERIOD 5 MINUTES
-- CREATE STREAM deleted_fct_bookings WITH (
--     kafka_topic = 'deleted_fct_bookings',
--     value_format = 'avro'
-- ) AS
-- SELECT 
--     br.booking,
--     br.room,
--     br.guest,
--     br.booking_room_updated_at,
--     b.checkin,
--     b.checkout,
--     b.booking_updated_at
-- FROM deleted_booking_rooms br
-- LEFT JOIN deleted_bookings b
-- WITHIN 5 MINUTES GRACE PERIOD 5 MINUTES
-- ON br.booking = b.id
-- EMIT CHANGES;

-- CREATE STREAM deleted_booking_rooms 
-- WITH (KAFKA_TOPIC='deleted_booking_rooms',VALUE_FORMAT='avro') AS 
-- SELECT before->booking, before->id, before->room, before->guest, FROM_UNIXTIME(before->updated_at) updated_at
-- FROM booking_rooms
-- WHERE before IS NOT NULL
-- PARTITION BY before->booking
-- EMIT CHANGES;

-- CREATE STREAM deleted_fct_bookings
-- WITH (KAFKA_TOPIC='deleted_fct_bookings',VALUE_FORMAT='avro') AS 
-- SELECT br.booking, br.room, br.guest, b.checkin, b.checkout
-- FROM deleted_booking_rooms AS br
-- LEFT JOIN deleted_bookings AS b
-- WITHIN 5 MINUTES 
-- ON b.id = br.booking
-- EMIT CHANGES;


-- CREATE STREAM deleted_bookings 
-- WITH (KAFKA_TOPIC='deleted_bookings',VALUE_FORMAT='avro') AS 
-- SELECT before->id, FROM_DAYS(before->checkin) checkin, FROM_DAYS(before->checkout) checkout, FROM_UNIXTIME(before->updated_at) updated_at
-- FROM bookings
-- WHERE before IS NOT NULL
-- PARTITION BY before->id
-- EMIT CHANGES;

-- CREATE TABLE deleted_bookings AS
-- WITH (KAFKA_TOPIC='deleted_fct_bookings23',VALUE_FORMAT='avro') AS
    -- ROWKEY,
    -- AS_VALUE(FROM_UNIXTIME(br.before->updated_at)) key3,
    -- AS_VALUE(FROM_UNIXTIME(b.before->updated_at)) key4,