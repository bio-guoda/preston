#! /bin/bash
#
# Archives GBIF dataset registry and associated endpoints .


set -x
TMPDIR=./tmp

function download_registry() {
  offset=0
  limit=50
  endOfRecords="false"

  while [ $endOfRecords = "false" ] 
  do
    registry_chunk=$(mktemp -p $TMPDIR)
    url="https://api.gbif.org/v1/dataset?offset=$offset&limit=$limit" 
    echo -n "[$url] downloading registry chunk... "
    curl -sL $url > $registry_chunk
    echo "done."

    endOfRecords=`cat $registry_chunk | jq .endOfRecords`
    cat $registry_chunk | jq -c .results[] | gzip >> $1
    offset=$[$offset+$limit]
    rm -f $registry_chunk
  done
}

function update_registry_cache() {
   
  STAGING_DIR=$TMPDIR/datasets
  rm -rf $STAGING_DIR
  mkdir -p $STAGING_DIR
  mkdir -p datasets

  REGISTRY_FILE=$1   

  zcat $REGISTRY_FILE | jq -r .key | sed -E 's/^(.{2})(.{2})(.{2})/\1\/\2\/\3\/\0/' > $TMPDIR/dataset-paths.txt

  cat $TMPDIR/dataset-paths.txt | xargs -L1 bash -c 'mkdir -p datasets/$0'

  zcat $REGISTRY_FILE | split -l 1 --additional-suffix=.json - $STAGING_DIR/dataset

  find $STAGING_DIR/dataset* | paste - $TMPDIR/dataset-paths.txt | xargs -L1 bash -c 'cp $0 datasets/$1/gbif.json'
}


function check_endpoint() {
  uuid=$1
  type=$2
  url=$3
  temp_file=$(mktemp -p tmp)
  workdir="datasets/$(echo $1 | sed -E 's/^(.{2})(.{2})(.{2})/\1\/\2\/\3\/\0/')"
  echo -n "HTTP\HEAD [$url] into tmp file [$temp_file]..."
  curl -ILs "$url" > $temp_file
  accessed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  hash=
  file_size=
  status=$?
  status_msg=FAIL
  echo " done with status [$status]."
  if [ $status -eq 0 ] ;then
    status_msg=OK
    file_size=$(cat $tmp_file | grep -i "Content-Size:" |  tr -c -d [:digit:])
    mv $temp_file $workdir/$hash.head
  fi
  #echo -e "https://www.gbif.org/dataset/$uuid\t$url\t$hash\t$accessed_at\t$type\t$status_msg\t$status" >> $workdir/access.tsv 

  jq -c -n --arg key $uuid --arg url $url --arg type $type --arg contentHash $hash --arg sizeInBytes $file_size --arg accessed_at $accessed_at --arg status_msg $status_msg --arg status_code $status '{"key": $key, "url": $url, "contentHashSHA256": $contentHash, "sizeInBytes": $sizeInBytes, "type": $type, "accessedAt" : $accessed_at, "statusMsg": $status_msg, "statusCode": $status_code }' >> $workdir/access.json

  rm -f $temp_file
}

export -f check_endpoint

function download_endpoint() {
  uuid=$1
  type=$2
  url=$3
  temp_file=$(mktemp -p tmp)
  workdir="datasets/$(echo $1 | sed -E 's/^(.{2})(.{2})(.{2})/\1\/\2\/\3\/\0/')"
  echo -n "downloading [$url] into tmp file [$temp_file]..."
  curl -Ls "$url" > $temp_file
  accessed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  hash=
  file_size=
  status=$?
  status_msg=FAIL
  echo " done with status [$status]."
  if [ $status -eq 0 ] ;then
    hash=$(sha256sum $temp_file | cut -d " " -f1)
    status_msg=OK
    file_size=$(stat -c%s $temp_file)
    mv $temp_file $workdir/$hash
  fi
  #echo -e "https://www.gbif.org/dataset/$uuid\t$url\t$hash\t$accessed_at\t$type\t$status_msg\t$status" >> $workdir/access.tsv 

  jq -c -n --arg key $uuid --arg url $url --arg type $type --arg contentHash $hash --arg sizeInBytes $file_size --arg accessed_at $accessed_at --arg status_msg $status_msg --arg status_code $status '{"key": $key, "url": $url, "contentHashSHA256": $contentHash, "sizeInBytes": $sizeInBytes, "type": $type, "accessedAt" : $accessed_at, "statusMsg": $status_msg, "statusCode": $status_code }' >> $workdir/access.json

  rm -f $temp_file
}

export -f download_endpoint 

function download_datasets() {
  endpoints=$(mktemp -p $TMPDIR)

  # generate list of all endpoint urls (may be multiple per dataset)
  find datasets/ | grep -e "gbif\.json" | xargs jq -r '.key as $top | .endpoints[] | {uuid: $top, type: .type, url: .url} | .uuid + "\t" + .type + "\t" + .url' | gzip > $endpoints

  #zcat $endpoints | head -n 2 | xargs -L1 bash -c 'download_endpoint $0 $1 $2'
  zcat $endpoints | head -n 2 | xargs -L1 bash -c 'check_endpoint $0 $1 $2'
  rm -f $endpoints
}

rm -rf $TMPDIR
mkdir -p $TMPDIR

REGISTRY_FILE=$TMPDIR/datasets.json.gz
#download_registry $REGISTRY_FILE
#update_registry_cache $REGISTRY_FILE

download_datasets
