#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"


# Call the primary GW for inserts
#curl -s localhost:6081/v1/add_documents -X POST -d '{
#   "indexName": "test_idx",
#   "fields":{"doc_id":{"fieldValue":[{"textValue":"144"}]},"license_no":{"fieldValue":[{"intValue":993},{"intValue":131}]},"vendor_name":{"fieldValue":[{"textValue":"extra"}]}}
#}' | jq .

NRTSEARCH_ROOT=$HOME/src/nrtsearch/nrtsearch
primary_client="${NRTSEARCH_ROOT}/build/install/nrtsearch/bin/lucene-client -h 127.0.0.1 -p 6000 "

$primary_client addDocuments -i test_idx -t csv -f <(cat <<EOF
doc_id,vendor_name,license_no
3,third vendor,300;360
4,fourth vendor,420;510
EOF
)




