#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"


#primary_client="docker run grpc-gateway:latest ./build/install/nrtsearch/bin/lucene-client -h 172.17.0.1 -p 6000 "
#replica1_client="docker run grpc-gateway:latest ./build/install/nrtsearch/bin/lucene-client -h 172.17.0.1 -p 7100 "

NRTSEARCH_ROOT=$HOME/src/nrtsearch/nrtsearch
primary_client="${NRTSEARCH_ROOT}/build/install/nrtsearch/bin/lucene-client -h 127.0.0.1 -p 6000 "
replica1_client="${NRTSEARCH_ROOT}/build/install/nrtsearch/bin/lucene-client -h 127.0.0.1 -p 7100 "

#
# Primary node setup
#
$primary_client createIndex --indexName test_idx
$primary_client settings -f <(cat <<EOF
{
  "indexName": "test_idx",
  "directory": "MMapDirectory",
  "nrtCachingDirectoryMaxSizeMB": -1.0,
  "indexMergeSchedulerAutoThrottle": false,
  "concurrentMergeSchedulerMaxMergeCount": 16,
  "concurrentMergeSchedulerMaxThreadCount": 8
}
EOF
)
$primary_client registerFields -f <(cat <<EOF
{             "indexName": "test_idx",
              "field":
              [
                      { "name": "doc_id", "type": "ATOM", "storeDocValues": true},
                      { "name": "vendor_name", "type": "TEXT" , "search": true, "store": true, "tokenize": true},
                      { "name": "license_no",  "type": "INT", "multiValued": true, "storeDocValues": true}
              ]
}
EOF
)

$primary_client startIndex -f <(cat <<EOF
{
  "indexName" : "test_idx",
  "mode": "PRIMARY"
}
EOF
)

#sleep 5

$primary_client addDocuments -i test_idx -t csv -f <(cat <<EOF
doc_id,vendor_name,license_no
0,first vendor,100;200
1,second vendor,111;222
EOF
)

#
# Replica-1 setup
#

$replica1_client createIndex -i test_idx

$replica1_client settings -f <(cat <<EOF
{
  "indexName": "test_idx",
  "directory": "MMapDirectory",
  "nrtCachingDirectoryMaxSizeMB": -1.0
}
EOF
)
$replica1_client registerFields -f <(cat <<EOF
{             "indexName": "test_idx",
              "field":
              [
                      { "name": "doc_id", "type": "ATOM", "storeDocValues": true},
                      { "name": "vendor_name", "type": "TEXT" , "search": true, "store": true, "tokenize": true},
                      { "name": "license_no",  "type": "INT", "multiValued": true, "storeDocValues": true}
              ]
}
EOF
)

# If running locally:
#  "primaryAddress": "127.0.0.1",
# If running in a local docker container:
#   "primaryAddress": "172.17.0.1",

$replica1_client startIndex -f <(cat <<EOF
{
  "indexName" : "test_idx",
  "mode": "REPLICA",
  "primaryAddress": "primary",
  "port": 6001
}
EOF
)


#sleep 1

#./build/install/nrtsearch/bin/lucene-client writeNRT -i test_idx -p 6001
#sleep 1

#$replica1_client search -f <(cat <<EOF
#{
#  "indexName": "test_idx",
#  "startHit": 0,
#  "topHits": 100,
#  "retrieveFields": ["doc_id", "license_no", "vendor_name"],
#  "queryText": "vendor_name:first vendor"
#}
#EOF
#)
 
