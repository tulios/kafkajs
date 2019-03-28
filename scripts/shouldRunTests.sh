#!/bin/bash -ex

CHANGED_FILES=`git diff --name-only master...COMMIT`

[[ -z $CHANGED_FILES ]] && exit 1

for CHANGED_FILE in $CHANGED_FILES; do
  if ! [[ "$CHANGED_FILE" =~ .md$ ]] && ! [[ "$CHANGED_FILE" =~ .png$ ]] && ! [[ "$CHANGED_FILE" =~ ^(docs|website|logo)/.*$ ]]; then
    exit 1
  fi
done
