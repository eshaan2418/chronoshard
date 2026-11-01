#!/bin/sh
# Differential test against a real Prometheus: start a throwaway Prometheus
# (remote-write receiver on) and chronoshard, push identical data to both,
# compare query results. Requires `prometheus` on PATH.
set -eu
OUT=${OUT:-bench/results/conformance.json}
SEED=${SEED:-1}
TMP=$(mktemp -d)
cleanup() {
  [ -n "${PROM_PID:-}" ] && kill "$PROM_PID" 2>/dev/null || true
  [ -n "${CS_PID:-}" ] && kill "$CS_PID" 2>/dev/null || true
  # chronoshard writes a final checkpoint on SIGTERM: let both exit first.
  wait 2>/dev/null || true
  rm -rf "$TMP" || true
}
trap cleanup EXIT
go build -o "$TMP/chronoshard" ./cmd/chronoshard
go build -o "$TMP/promcompare" ./cmd/promcompare
printf 'global:\n  scrape_interval: 1h\n' > "$TMP/prometheus.yml"
prometheus --config.file="$TMP/prometheus.yml" --storage.tsdb.path="$TMP/prom" \
  --web.listen-address=127.0.0.1:19090 --web.enable-remote-write-receiver \
  --log.level=error > "$TMP/prom.log" 2>&1 &
PROM_PID=$!
"$TMP/chronoshard" -data "$TMP/cs" -listen 127.0.0.1:19201 > "$TMP/cs.log" 2>&1 &
CS_PID=$!
for i in $(seq 1 60); do
  curl -sf http://127.0.0.1:19090/-/ready >/dev/null 2>&1 && curl -sf http://127.0.0.1:19201/-/healthy >/dev/null 2>&1 && break
  sleep 0.5
done
mkdir -p "$(dirname "$OUT")"
"$TMP/promcompare" -prom http://127.0.0.1:19090 -ours http://127.0.0.1:19201 -seed "$SEED" -out "$OUT" ${VERBOSE:+-v}
