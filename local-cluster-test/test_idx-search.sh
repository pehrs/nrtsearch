#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

NRTSEARCH_ROOT=$HOME/src/nrtsearch/nrtsearch
primary_client="${NRTSEARCH_ROOT}/build/install/nrtsearch/bin/lucene-client -h 127.0.0.1 -p 6000 "
replica1_client="${NRTSEARCH_ROOT}/build/install/nrtsearch/bin/lucene-client -h 127.0.0.1 -p 7100 "

client="$primary_client"

if [[ "$use_client" == "replica1" ]]; then
  client="$replica1_client"
fi


curl_rest() {
  # Call the replica GW
  curl -s localhost:6081/v1/search -X POST -d '{
      "indexName": "test_idx",
      "startHit": 0,
      "topHits": 7,
      "retrieveFields": ["doc_id", "license_no", "vendor_name"],
      "queryText": "doc_id:4 and vendor_name:ven*"
      }' |  jq -c .hits[]
}


client_rest() {
  $client search -f <(cat <<EOF
{
      "indexName": "test_idx",
      "startHit": 0,
      "topHits": 7,
      "retrieveFields": ["doc_id", "license_no", "vendor_name"],
      "queryText": "doc_id:4 and vendor_name:ven*"
}
EOF
)

}

client_rest
