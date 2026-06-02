#!/bin/bash -e

COMPOSE_FILE=${COMPOSE_FILE:="docker-compose.2_4.yml"}

echo "Running compose file: ${COMPOSE_FILE}:"
"${PWD}/scripts/dockerCompose.sh" -f "${COMPOSE_FILE}" up --force-recreate -d
if [ -z ${NO_LOGS} ]; then
  "${PWD}/scripts/dockerCompose.sh" -f "${COMPOSE_FILE}" logs -f
fi
