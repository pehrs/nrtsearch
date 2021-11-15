#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

# Call the replica GW
curl -s localhost:6081/v1/search -X POST -d '{
   "indexName": "test_idx",
   "startHit": 0,
   "topHits": 7,
   "retrieveFields": ["doc_id", "license_no", "vendor_name"],
   "queryText": "doc_id:4 and vendor_name:ven*"
}' |  jq -c .hits[]
