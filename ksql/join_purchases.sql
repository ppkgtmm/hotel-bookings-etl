SET 'auto.offset.reset' = 'earliest';
SET 'cache.max.bytes.buffering' = '0';

CREATE STREAM ba_purchase WITH (
    kafka_topic = 'booking_addons',
    value_format = 'avro'

);

CREATE STREAM br_purchase WITH (
    kafka_topic = 'booking_rooms',
    value_format = 'avro'
);

CREATE TABLE brb_purchase AS
SELECT
    before->id,
    EARLIEST_BY_OFFSET(before->room) room,
    EARLIEST_BY_OFFSET(before->guest) guest,
    EARLIEST_BY_OFFSET(before->updated_at) updated_at
FROM br_purchase
WINDOW TUMBLING (SIZE 30 SECONDS)
WHERE before IS NOT NULL
GROUP BY before->id
EMIT FINAL;

CREATE TABLE bab_purchase AS
SELECT
    before->id,
    EARLIEST_BY_OFFSET(before->booking_room) booking_room,
    EARLIEST_BY_OFFSET(before->addon) addon,
    EARLIEST_BY_OFFSET(before->quantity) quantity,
    EARLIEST_BY_OFFSET(before->datetime) datetime,
    EARLIEST_BY_OFFSET(before->updated_at) updated_at
FROM ba_purchase
WINDOW TUMBLING (SIZE 30 SECONDS)
WHERE before IS NOT NULL
GROUP BY before->id
EMIT FINAL;

CREATE TABLE bra_purchase AS
SELECT
    after->id,
    LATEST_BY_OFFSET(after->room) room,
    LATEST_BY_OFFSET(after->guest) guest,
    LATEST_BY_OFFSET(after->updated_at) updated_at
FROM br_purchase
WINDOW TUMBLING (SIZE 30 SECONDS)
WHERE after IS NOT NULL
GROUP BY after->id
EMIT FINAL;

CREATE TABLE baa_purchase AS
SELECT
    after->id,
    LATEST_BY_OFFSET(after->booking_room) booking_room,
    LATEST_BY_OFFSET(after->addon) addon,
    LATEST_BY_OFFSET(after->quantity) quantity,
    LATEST_BY_OFFSET(after->datetime) datetime,
    LATEST_BY_OFFSET(after->updated_at) updated_at
FROM ba_purchase
WINDOW TUMBLING (SIZE 30 SECONDS)
WHERE after IS NOT NULL
GROUP BY after->id
EMIT FINAL;
