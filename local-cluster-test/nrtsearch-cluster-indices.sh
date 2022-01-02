#!/bin/bash


endpoint=${1:primary}

ep=localhost:6000
if [[ $endpoint == "replica1" ]]; then
  ep=localhost:7100
fi


grpcurl \
  -max-time 5 \
  -plaintext \
  --d '{}' \
  $ep \
  luceneserver.LuceneServer/indices

