#!/bin/bash
if [ -n "$TRAVIS_TAG" ]; then
  VERSION=${TRAVIS_TAG//[^0-9.]/}
  mvn versions:set -DnewVersion=$VERSION -am -pl preston-cli
  mvn versions:set -DnewVersion=$VERSION
fi
