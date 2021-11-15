#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

docker run \
       -d \
       --name gw \
       -p 6080:6080 \
       grpc-gateway:latest \
       ./bin/http_wrapper-linux-386 172.17.0.1:6000 :6080
