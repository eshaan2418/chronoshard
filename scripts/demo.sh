#!/bin/sh
# Push the same synthetic workload to chronoshard and a real Prometheus,
# then run one query against both. Requires `prometheus` and `jq`.
set -eu
TMP=$(mktemp -d)
cleanup() { kill $(cat "$TMP"/pids) 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT
go build -o "$TMP/chronoshard" ./cmd/chronoshard
go build -o "$TMP/promcompare" ./cmd/promcompare
printf 'global:\n  scrape_interval: 1h\n' > "$TMP/p.yml"
( prometheus --config.file="$TMP/p.yml" --storage.tsdb.path="$TMP/prom" --web.listen-address=127.0.0.1:19090 \
    --web.enable-remote-write-receiver --log.level=error >/dev/null 2>&1 & echo $! >> "$TMP/pids" )
( "$TMP/chronoshard" -data "$TMP/cs" -listen 127.0.0.1:19201 2>/dev/null & echo $! >> "$TMP/pids" )
sleep 3
echo "# same data in both; 47 queries x 5 evaluations compared:"
"$TMP/promcompare" -prom http://127.0.0.1:19090 -ours http://127.0.0.1:19201
Q='topk(3, sum by (job, code) (rate(http_requests_total[5m])))'
T=$(date +%s)  # evaluate both at the same instant
for who in chronoshard:19201 prometheus:19090; do
  echo "# $Q   <- ${who%%:*}"
  curl -s "127.0.0.1:${who##*:}/api/v1/query" --data-urlencode "query=$Q" --data-urlencode "time=$T" | jq -r '.data.result[] | "\(.metric.job) \(.metric.code)  \(.value[1])"' | sort
done
