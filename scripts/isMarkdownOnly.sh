#!/bin/bash

set -e

CHANGED_FILES=`git diff --name-only master...${TRAVIS_COMMIT}`

[[ -z $CHANGED_FILES ]] && exit 1

for CHANGED_FILE in $CHANGED_FILES; do
  if ! [[ $CHANGED_FILE =~ .md$ ]]; then
    exit 1
  fi
done
