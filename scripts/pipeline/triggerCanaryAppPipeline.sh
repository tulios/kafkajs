#!/bin/bash -ex

curl \
  -X POST \
  -H "Authorization: token ${GH_TOKEN}" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/kafkajs/canary-app/actions/workflows/2502964/dispatches \
  -d "{\"ref\": \"master\", \"inputs\": { \"kafkajs_version\":\"${PRE_RELEASE_VERSION:-$BASH2_PRE_RELEASE_VERSION}\" }}"
