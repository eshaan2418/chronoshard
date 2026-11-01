#!/bin/sh
# Ingestion (in-process) and query latency vs Prometheus (both over HTTP,
# identical data). Results go to bench/results/.
set -eu
export GOMAXPROCS=${GOMAXPROCS:-9}
TMP=$(mktemp -d)
cleanup() {
  [ -n "${PROM_PID:-}" ] && kill "$PROM_PID" 2>/dev/null || true
  [ -n "${CS_PID:-}" ] && kill "$CS_PID" 2>/dev/null || true
  # chronoshard writes a final checkpoint on SIGTERM: let both exit first.
  wait 2>/dev/null || true
  rm -rf "$TMP" || true
}
trap cleanup EXIT
mkdir -p bench/results
go build -o "$TMP/chronobench" ./cmd/chronobench
go build -o "$TMP/chronoshard" ./cmd/chronoshard
{
  echo "# $(date '+%Y-%m-%d %H:%M') $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m) $(go version | cut -d' ' -f3) prometheus $(prometheus --version 2>&1 | head -1 | cut -d' ' -f3) GOMAXPROCS=$GOMAXPROCS"
  "$TMP/chronobench" ingest -series 10000 -samples 480 -runs 5 -workers 4
} | tee bench/results/ingest.txt
printf 'global:\n  scrape_interval: 1h\n' > "$TMP/p.yml"
prometheus --config.file="$TMP/p.yml" --storage.tsdb.path="$TMP/prom" --web.listen-address=127.0.0.1:19090 \
  --web.enable-remote-write-receiver --log.level=error > /dev/null 2>&1 &
PROM_PID=$!
"$TMP/chronoshard" -data "$TMP/cs" -listen 127.0.0.1:19201 > /dev/null 2>&1 &
CS_PID=$!
sleep 3
"$TMP/chronobench" query -series 2000 -samples 240 -reps 20 | tee bench/results/query.txt
echo "rss_kb prometheus=$(ps -o rss= -p $PROM_PID | tr -d ' ') chronoshard=$(ps -o rss= -p $CS_PID | tr -d ' ')" | tee -a bench/results/query.txt
