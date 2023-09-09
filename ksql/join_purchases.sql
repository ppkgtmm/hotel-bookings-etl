SET 'auto.offset.reset' = 'earliest';
SET 'cache.max.bytes.buffering' = '0';

CREATE STREAM booking_addons WITH (
    kafka_topic = 'booking_addons',
    value_format = 'avro'
);