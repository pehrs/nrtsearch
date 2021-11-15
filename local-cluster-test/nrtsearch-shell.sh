#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

docker run \
       -ti \
       grpc-gateway:latest \
       /bin/bash
