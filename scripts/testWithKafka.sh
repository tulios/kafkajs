#!/bin/bash -e

testCommand="$1"
extraArgs="$2"

wait_for_kafka() {
  docker exec \
    ${CONTAINER_ID} \
    bash -c "JMX_PORT=9998 /kafka/bin/kafka-topics.sh --zookeeper zk:2181 --list 2> /dev/null"
  sleep 3 # this seems to be ready a tidy bit before the actual kafka api
}

create_topic() {
  docker exec \
    ${CONTAINER_ID} \
    bash -c "JMX_PORT=9998 /kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-topic-already-exists --replication-factor 1 --partitions 2 --zookeeper zk:2181 2> /dev/null"
}

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=com.docker.compose.project=kafkaprotocol" \
    --filter "label=com.docker.compose.service=kafka" \
    --no-trunc \
    -q)
}

quit() {
  if [ ! -z ${CONTAINER_ID} ]; then
    echo -e "Stopping kafka docker container"
    docker-compose down
  fi
  exit 1
}

if [ -z ${DO_NOT_STOP} ]; then
  trap quit ERR
fi

CONTAINER_ID=$(find_container_id)

if [ -z ${CONTAINER_ID} ]; then
  echo -e "Start kafka docker container"
  docker-compose up --force-recreate -d
  if [ "1" = "$?" ]; then
    echo -e "Failed to start kafka image"
    exit 1
  fi

  CONTAINER_ID=$(find_container_id)
fi

wait_for_kafka
echo -e "Kafka container ${CONTAINER_ID} is running"
create_topic
echo

eval "${testCommand} ${extraArgs}"
TEST_EXIT=$?
echo

if [ -z ${DO_NOT_STOP} ]; then
  docker-compose down
fi
exit ${TEST_EXIT}
