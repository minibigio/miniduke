#!/bin/sh
my_dir=`dirname $0`
curl -H 'Content-Type: application/x-ndjson' -XPOST '127.0.0.1:9200/_bulk?pretty' --data-binary @$my_dir/miniduke_test.json
