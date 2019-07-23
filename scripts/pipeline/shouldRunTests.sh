#!/bin/bash -ex

LAST_COMMIT=$(git log --format="%H" -n 1)
CHANGED_FILES=`git diff --name-only master...${LAST_COMMIT}`

echo -d "LAST_COMMIT=${LAST_COMMIT}"
[[ -z $CHANGED_FILES ]] && exit 1

echo 'Changed files:'
echo -d $CHANGED_FILES | tr ' ' '\n'

for CHANGED_FILE in $CHANGED_FILES; do
  if ! [[ "$CHANGED_FILE" =~ .md$ ]] && ! [[ "$CHANGED_FILE" =~ .png$ ]] && ! [[ "$CHANGED_FILE" =~ ^(docs|website|logo)/.*$ ]]; then
    exit 1
  fi
done
