#!/bin/bash

set -e

CHANGED_FILES=`git diff --name-only master...$(Build.SourceVersion)`

[[ -z $CHANGED_FILES ]] && exit 1

for CHANGED_FILE in $CHANGED_FILES; do
  if ! [[ "$CHANGED_FILE" =~ .md$ ]] && ! [[ "$CHANGED_FILE" =~ ^(docs|website)/.*$ ]]; then
    exit 1
  fi
done
