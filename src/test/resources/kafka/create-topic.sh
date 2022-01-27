./kafka-topics --bootstrap-server localhost:9092 --create --topic users --replication-factor 1 --partitions 1


./kafka-avro-console-producer --topic users --bootstrap-server localhost:9092 --property value.schema="$(< /Users/hayssams/git/public/starlake/src/main/resources/kafka/user.avsc)"

./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic users \
  --property value.schema='{ "type": "record", "name": "User", "namespace": "ai.starlake.kafka.sample", "fields": [{"name": "first","type": "string"},{"name": "last","type": "string"},{"name": "timestamp","type": "long"}]}'

{ "first": "hayssam1", "last": "saleh1", "timestamp": 1643107581 }
{ "first":"hayssam2", "last":"saleh2", "timestamp":1643107582}
{ "first":"hayssam3", "last":"saleh3", "timestamp":1643107583}
{ "first":"hayssam4", "last":"saleh4", "timestamp":1643107584}

./kafka-avro-console-consumer --topic users --bootstrap-server localhost:9092  --from-beginning
