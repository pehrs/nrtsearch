#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

NRTSEARCH_ROOT=$HOME/src/nrtsearch/nrtsearch
client="${NRTSEARCH_ROOT}/build/install/nrtsearch/bin/lucene-client -h 127.0.0.1 -p 6000 "

# The docker way
#primary_client="docker run grpc-gateway:latest ./build/install/nrtsearch/bin/lucene-client -h 172.17.0.1 -p 6000 "

# The script way
$client $@
