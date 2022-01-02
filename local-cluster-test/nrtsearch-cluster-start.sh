#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"


setup_net() {
  docker network inspect nrtsearch-net &> /dev/null
  if [[ "$?" == "1" ]]; then
    docker network create nrtsearch-net
  fi
}

echo "Create nrtsearch network"
setup_net

echo "Start server and server gw"
$script_dir/primary-server-start.sh
$script_dir/primary-gw-start.sh

echo "Start replica and replica gw"
$script_dir/replica1-server-start.sh
$script_dir/replica1-gw-start.sh

#echo "Wait 3 seconds..."
#sleep 3

echo "Create and populate index"
$script_dir/test_idx-init.sh
