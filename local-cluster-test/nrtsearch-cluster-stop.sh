#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"


echo "Killing containers..."
nohup docker kill primary primary-gw replica1 replica1-gw &> /dev/null

echo "Removing containers..."
nohup docker rm primary primary-gw replica1 replica1-gw &> /dev/null

echo "Removing network..."
nohup docker network rm nrtsearch-net

echo "done!"
