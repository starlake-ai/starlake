curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{ "schema": "{ \"type\": \"record\", \"name\": \"User\", \"namespace\": \"ai.starlake.kafka.sample\", \"fields\": [ { \"name\": \"first\", \"type\": \"string\" }, { \"name\": \"last\", \"type\": \"string\" }, { \"name\": \"timestamp\", \"type\": \"long\" } ]}" }' \
  http://localhost:8081/subjects/users-value/versions


