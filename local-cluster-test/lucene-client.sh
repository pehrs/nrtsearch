#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

# The docker way
#primary_client="docker run grpc-gateway:latest ./build/install/nrtsearch/bin/lucene-client -h 172.17.0.1 -p 6000 "

# The script way
primary_client="$script_dir/../build/install/nrtsearch/bin/lucene-client -h 172.0.0.1 -p 6000 "

$primary_client $@
