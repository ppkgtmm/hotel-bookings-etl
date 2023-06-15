FROM confluentinc/cp-kafka-connect-base:7.3.2

RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:2.2.1

CMD [ "/etc/confluent/docker/run" ]
