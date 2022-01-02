#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

docker run \
       -d \
       --net nrtsearch-net \
       --name primary \
       -p 6000:6000 \
       -p 6001:6001 \
       -v ${script_dir}/primary.yaml:/primary.yaml \
       -v /spotify:/spotify \
       grpc-gateway:latest \
       ./build/install/nrtsearch/bin/lucene-server /primary.yaml
