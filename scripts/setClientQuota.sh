#!/bin/bash -e

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafkajs" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q)
}

CLIENT_ID=${CLIENT_ID:='example-producer'}
PRODUCER_BYTES=${PRODUCER_BYTES:=1024}
CONSUMER_BYTES=${CONSUMER_BYTES:=1024}

echo 'Setting client quota for:'
echo "CLIENT_ID: '${CLIENT_ID}'"
echo "PRODUCER_BYTES: ${PRODUCER_BYTES}"
echo "CONSUMER_BYTES: ${CONSUMER_BYTES}"

docker exec \
 $(find_container_id) \
 bash -c "kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'producer_byte_rate=${PRODUCER_BYTES},consumer_byte_rate=${CONSUMER_BYTES}' --entity-type clients --entity-name ${CLIENT_ID}"
