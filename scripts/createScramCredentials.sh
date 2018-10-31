#!/bin/bash -e

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafkajs" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q)
}

USERNAME=${USERNAME:='testscram'}
PASSWORD_256=${PASSWORD_256:='testtestscram256'}
PASSWORD_512=${PASSWORD_512:='testtestscram512'}

docker exec \
  $(find_container_id) \
  bash -c "/opt/kafka/bin/kafka-configs.sh --zookeeper zk:2181 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=${PASSWORD_256}],SCRAM-SHA-512=[password=${PASSWORD_512}]' --entity-type users --entity-name ${USERNAME}"
