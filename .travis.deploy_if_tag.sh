#!/bin/bash
if [ -n "$TRAVIS_TAG" ]; then
  mvn -s .travis.maven.settings.xml -DskipTests clean deploy
  VERSION=${TRAVIS_TAG//[^0-9.]/}
  cp target/preston-${VERSION}-jar-with-dependencies.jar preston_no_head.jar
  cat .travis.jar.magic preston_no_head.jar > preston.jar
fi
