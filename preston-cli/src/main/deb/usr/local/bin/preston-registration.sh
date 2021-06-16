#!/bin/bash
# Register all preston urls with hash-archive.org
#
# please replace deeplinker.bio with you own hostname.
#
# see https://preston.guoda.bio on how to install preston
#

HOSTNAME=deeplinker.bio # please replace [deeplinker.bio] with https endpoint that serves your preston archive

preston ls -l tsv | grep Version | cut -f1,3 | tr '\t' '\n' | grep -v "deeplinker\.bio/\.well-known/genid" | sort | uniq | sed -e "s/hash:\/\/sha256/https:\/\/$HOSTNAME/g" | sed -e 's/^/https:\/\/hash-archive.org\/api\/enqueue\//g' | xargs -L1 curl 
