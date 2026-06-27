FROM confluentinc/cp-kafka-connect:7.6.0

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6
