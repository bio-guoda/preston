#!/bin/bash
PRESTON_CLI_DIR="${TRAVIS_BUILD_DIR}/preston-cli/target"
if [ -n "$TRAVIS_TAG" ]; then
  mkdir -p target ${TRAVIS_BUILD_DIR}/target
  mvn -s ${TRAVIS_BUILD_DIR}/.travis/maven.settings.xml -DskipTests clean deploy
  VERSION=${TRAVIS_TAG//[^0-9.]/}
  cp ${PRESTON_CLI_DIR}/preston-${VERSION}-jar-with-dependencies.jar ${TRAVIS_BUILD_DIR}/target/preston_no_head.jar
  cat ${TRAVIS_BUILD_DIR}/.travis/jar_magic.sh ${TRAVIS_BUILD_DIR}/target/preston_no_head.jar > target/preston.jar
  mvn jdeb:jdeb
  mv ${PRESTON_CLI_DIR}/target/jib-image.tar ${TRAVIS_BUILD_DIR}/target/preston.image.tar
fi
