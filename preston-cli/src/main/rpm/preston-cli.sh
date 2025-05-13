#!/usr/bin/env bash

java -Xmx4G -XX:+UseG1GC -cp "/usr/local/preston/lib/*" bio.guoda.preston.Preston "$@"