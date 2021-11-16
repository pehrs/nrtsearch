#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

docker run \
       -d \
       --name replica1-gw \
       -p 6081:6080 \
       -p 8081:8088 \
       grpc-gateway:latest \
       ./bin/http_wrapper-linux-386 172.17.0.1:7100 :6080 :8088
