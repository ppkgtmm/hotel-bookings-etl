# install connector
confluent-hub install --component-dir ${CONNECT_PLUGIN_PATH} --no-prompt debezium/debezium-connector-mysql:2.2.1

# start kafka connect
/etc/confluent/docker/run
