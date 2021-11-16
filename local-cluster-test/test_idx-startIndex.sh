#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)"

# FIXME: This is WIP, I'm trying to figure out what is needed when primary and replica servers restart.

# Seems like we have to start an index if the servers are restarted...

# Primary
curl -s localhost:6080/v1/start_index -X POST -d '{
  "indexName": "test_idx",
  "mode": "PRIMARY"
}' | jq .

# Replica1 Register and start
curl -s localhost:6081/v1/register_fields -X POST -d '{
  "indexName": "test_idx",
  "field":
    [
       { "name": "doc_id", "type": "ATOM", "storeDocValues": true},
       { "name": "vendor_name", "type": "TEXT" , "search": true, "store": true, "tokenize": true},
       { "name": "license_no",  "type": "INT", "multiValued": true, "storeDocValues": true}
    ]
}' | jq .

# When running with docker:
#  "primaryAddress": "172.17.0.1",
# When running local scripts:
#  "primaryAddress": "127.0.0.1",

curl -s localhost:6081/v1/start_index -X POST -d '{
  "indexName": "test_idx",
  "mode": "REPLICA",
  "primaryAddress": "172.17.0.1",
  "port": 6001
}' | jq .
