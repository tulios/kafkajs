#!/bin/bash -e

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafkajs" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q)
}

ZOOKEEPER_USERNAME=${ZOOKEEPER_USERNAME:='testscram'}
PASSWORD_256=${PASSWORD_256:='testtestscram=256'}
PASSWORD_512=${PASSWORD_512:='testtestscram=512'}

docker exec \
  $(find_container_id) \
  bash -c "kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=${PASSWORD_256}],SCRAM-SHA-512=[password=${PASSWORD_512}]' --entity-type users --entity-name ${ZOOKEEPER_USERNAME}"

echo "The credentials have been setup as following"
echo "ZOOKEEPER_USERNAME: ${ZOOKEEPER_USERNAME}"
echo "PASSWORD_256: ${PASSWORD_256}"
echo "PASSWORD_512: ${PASSWORD_512}"