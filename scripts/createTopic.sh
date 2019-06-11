#!/bin/bash -e

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafkajs" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q)
}

TOPIC=${TOPIC:='test-topic'}
PARTITIONS=${PARTITIONS:=3}

docker exec \
  $(find_container_id) \
  bash -c "kafka-topics --create --if-not-exists --topic ${TOPIC} --replication-factor 1 --partitions ${PARTITIONS} --zookeeper zookeeper:2181"
