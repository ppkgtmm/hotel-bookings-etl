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

CREATE STREAM booking_addons WITH (
    kafka_topic = 'booking_addons',
    value_format = 'avro'
);

CREATE TABLE bookings_before AS
SELECT
    before->id,
    EARLIEST_BY_OFFSET(before->checkin) checkin,
    EARLIEST_BY_OFFSET(before->checkout) checkout
FROM bookings
WINDOW TUMBLING (SIZE 2 MINUTES)
WHERE before IS NOT NULL
GROUP BY before->id
EMIT FINAL;

CREATE TABLE bookings_after AS
SELECT
    after->id,
    LATEST_BY_OFFSET(after->checkin) checkin,
    LATEST_BY_OFFSET(after->checkout) checkout
FROM bookings
WINDOW TUMBLING (SIZE 2 MINUTES)
WHERE after IS NOT NULL
GROUP BY after->id
EMIT FINAL;

CREATE TABLE booking_rooms_before AS
SELECT
    before->id,
    EARLIEST_BY_OFFSET(before->booking) booking,
    EARLIEST_BY_OFFSET(before->room) room,
    EARLIEST_BY_OFFSET(before->guest) guest,
    EARLIEST_BY_OFFSET(before->updated_at) updated_at
FROM booking_rooms
WINDOW TUMBLING (SIZE 2 MINUTES)
WHERE before IS NOT NULL
GROUP BY before->id
EMIT FINAL;

CREATE TABLE booking_rooms_after AS
SELECT
    after->id,
    LATEST_BY_OFFSET(after->booking) booking,
    LATEST_BY_OFFSET(after->room) room,
    LATEST_BY_OFFSET(after->guest) guest,
    LATEST_BY_OFFSET(after->updated_at) updated_at
FROM booking_rooms
WINDOW TUMBLING (SIZE 2 MINUTES)
WHERE after IS NOT NULL
GROUP BY after->id
EMIT FINAL;

CREATE TABLE booking_addons_before AS
SELECT
    before->id,
    EARLIEST_BY_OFFSET(before->booking_room) booking_room,
    EARLIEST_BY_OFFSET(before->addon) addon,
    EARLIEST_BY_OFFSET(before->quantity) quantity,
    EARLIEST_BY_OFFSET(before->datetime) datetime,
    EARLIEST_BY_OFFSET(before->updated_at) updated_at
FROM booking_addons
WINDOW TUMBLING (SIZE 2 MINUTES)
WHERE before IS NOT NULL
GROUP BY before->id
EMIT FINAL;

CREATE TABLE booking_addons_after AS
SELECT
    after->id,
    LATEST_BY_OFFSET(after->booking_room) booking_room,
    LATEST_BY_OFFSET(after->addon) addon,
    LATEST_BY_OFFSET(after->quantity) quantity,
    LATEST_BY_OFFSET(after->datetime) datetime,
    LATEST_BY_OFFSET(after->updated_at) updated_at
FROM booking_addons
WINDOW TUMBLING (SIZE 2 MINUTES)
WHERE after IS NOT NULL
GROUP BY after->id
EMIT FINAL;
