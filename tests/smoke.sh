#!/bin/sh
set -eu
bin="$1"
port=50091
"$bin" server --port "$port" --workers 2 --capacity 16 --work-us 100 > /dev/null 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT INT TERM
sleep 1
"$bin" client --port "$port" --count 100 --concurrency 4 --payload-bytes 1024 --deadline-ms 2000 | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["ok"] == 100 and x["errors"] == 0 and x["deadline"] == 0, x'
"$bin" client --port "$port" --count 3 --concurrency 1 --payload-bytes 1024 --corrupt-checksum 1 | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["data_loss"] == 3 and x["ok"] == 0, x'
