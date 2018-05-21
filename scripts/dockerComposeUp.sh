#!/bin/bash -e

export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)

COMPOSE_FILE=${COMPOSE_FILE:="docker-compose.yml"}

docker-compose -f "${COMPOSE_FILE}" up --force-recreate -d
if [ -z ${NO_LOGS} ]; then
  docker-compose logs -f
fi
