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

CREATE STREAM bookings_after_repart WITH (kafka_topic='bookings_after_repart', key_format='avro', value_format='avro') AS
SELECT
    after->id,
    after->checkin,
    after->checkout,
    after->updated_at
FROM bookings
WHERE after IS NOT NULL
PARTITION BY after->id;

CREATE STREAM booking_rooms_before_repart WITH (kafka_topic='booking_rooms_before_repart', key_format='avro', value_format='avro') AS
SELECT
    before->booking,
    before->id,
    before->room,
    before->guest,
    before->updated_at
FROM booking_rooms
WHERE before IS NOT NULL
PARTITION BY before->booking;

CREATE STREAM booking_rooms_after_repart WITH (kafka_topic='booking_rooms_after_repart', key_format='avro', value_format='avro') AS
SELECT
    after->booking,
    after->id,
    after->room,
    after->guest,
    after->updated_at
FROM booking_rooms
WHERE after IS NOT NULL
PARTITION BY after->booking;

CREATE STREAM bookings_before WITH (kafka_topic='bookings_before', key_format='avro', value_format='avro') AS
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
ON br.booking = b.id;

CREATE STREAM bookings_after WITH (kafka_topic='bookings_after', key_format='avro', value_format='avro') AS
SELECT
    br.booking,
    br.id,
    br.room,
    br.guest,
    b.checkin,
    b.checkout,
    br.updated_at booking_room_updated,
    b.updated_at booking_updated
FROM booking_rooms_after_repart br
LEFT JOIN bookings_after_repart b
WITHIN 5 MINUTES GRACE PERIOD 5 MINUTES
ON br.booking = b.id;
