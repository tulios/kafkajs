#!/bin/bash -e

if docker compose version >/dev/null 2>&1; then
  exec docker compose "$@"
fi

if command -v docker-compose >/dev/null 2>&1; then
  exec docker-compose "$@"
fi

echo "Neither 'docker compose' nor 'docker-compose' is available" >&2
exit 127
