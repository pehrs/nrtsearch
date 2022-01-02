#!/bin/bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"
#       -v ${script_dir}/primary.conf:/code/src/main/resources/lucene_server_default_configuration.yaml \

docker run \
       -d \
       --net nrtsearch-net \
       --name replica1 \
       -p 7100:7100 \
       -p 7101:7101 \
       -v ${script_dir}/replica1.yaml:/replica.yaml \
       -v /spotify:/spotify \
       grpc-gateway:latest \
       ./build/install/nrtsearch/bin/lucene-server /replica.yaml
