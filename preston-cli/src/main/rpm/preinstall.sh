#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

function verify_java_version_min {
    minimal_version=8
    current_version=$(java -version 2>&1 | tr "\n" " " | cut -d " " -f 3  | tr -d '"' | cut -d "." -f 1)
    if (( current_version < minimal_version ))
    then
      return 1;
    fi
    return 0
}

# Verify the existence of java command, if not there verification should exit already
type -P java &> /dev/null || { echo "java not found; please install java using [sudo dnf install java-21-openjdk.x86_64] (fedora) or similar."; exit 1; }

verify_java_version_min
exit_code=$?

exit $exit_code