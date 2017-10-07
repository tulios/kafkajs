#!/bin/bash -e

testCommand="$1"
extraArgs="$2"

export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)

wait_for_kafka() {
  docker exec \
    ${CONTAINER_ID} \
    bash -c "JMX_PORT=9998 /opt/kafka/bin/kafka-topics.sh --zookeeper zk:2181 --list 2> /dev/null"
  sleep 3 # this seems to be ready a tidy bit before the actual kafka api
}

create_topic() {
  docker exec \
    ${CONTAINER_ID} \
    bash -c "JMX_PORT=9998 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-topic-already-exists --replication-factor 1 --partitions 2 --zookeeper zk:2181 2> /dev/null"
}

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=com.docker.compose.project=kafkaprotocol" \
    --filter "label=com.docker.compose.service=kafka1" \
    --no-trunc \
    -q)
}

quit() {
  if [ ! -z ${CONTAINER_ID} ]; then
    echo -e "Stopping kafka docker container"
    docker-compose down --remove-orphans
  fi
  exit 1
}

if [ -z ${DO_NOT_STOP} ]; then
  trap quit ERR
fi

CONTAINER_ID=$(find_container_id)

if [ -z ${CONTAINER_ID} ]; then
  echo -e "Start kafka docker container"
  NO_LOGS=1 ./scripts/dockerComposeUp.sh
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
  docker-compose down --remove-orphans
fi
exit ${TEST_EXIT}
