#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

curl -s localhost:6080/v1/search -X POST -d '{
   "indexName": "test_idx",
   "startHit": 0,
   "topHits": 100,
   "retrieveFields": ["doc_id", "license_no", "vendor_name"],
   "queryText": "vendor_name:first vendor"
}' | jq .
