SET 'auto.offset.reset' = 'earliest';

DROP CONNECTOR IF EXISTS ${DBZ_CONNECTOR};

CREATE SOURCE CONNECTOR ${DBZ_CONNECTOR} WITH (
    'connector.class' = 'io.debezium.connector.mysql.MySqlConnector',
    'tasks.max' = '1', 
    'database.hostname' = '${DB_HOST_INTERNAL}',
    'database.port' = '${DB_PORT}',
    'database.user' = '${DBZ_USER}', 
    'database.password' = '${DBZ_PASSWORD}',
    'database.server.id' = 1,
    'database.allowPublicKeyRetrieval' = true,
    'topic.prefix' = '${OLTP_DB}',
    'database.include.list' = '${OLTP_DB}', 
    'table.include.list' = '${OLTP_DB}.*',
    'database.history.kafka.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS_INTERNAL}',
    'database.history.kafka.topic' = 'history.${OLTP_DB}',
    'min.row.count.to.stream.results' = 0,
    'snapshot.mode' = 'schema_only',
    "transforms" = "dropPrefix",
    "transforms.dropPrefix.type" = "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.dropPrefix.regex" = "(.*)\\.(.*)\\.(.*)",
    "transforms.dropPrefix.replacement" = "$3"
);
