#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

curl -s localhost:6080/v1/backup_index -X POST -d '{
   "indexName": "test_idx",
   "serviceName": "svc",
   "resourceName": "res1",
   "completeDirectory": true,
   "stream": false
}' | jq .
