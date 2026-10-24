#!/usr/bin/env bash
# Runs a local five node cluster, one JVM per node.
#   scripts/cluster.sh start          build the jar and start n1..n5
#   scripts/cluster.sh status         show each node's role, term and commit index
#   scripts/cluster.sh kill n3        kill -9 one node (simulates a crash)
#   scripts/cluster.sh restart n3     start a killed node again from its data directory
#   scripts/cluster.sh stop           stop everything (data is kept in ./data)
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003,n4=127.0.0.1:7004:8004,n5=127.0.0.1:7005:8005"
NODES="n1 n2 n3 n4 n5"
mkdir -p data

start_node() {
  local id=$1
  java -jar raftkv.jar --id "$id" --cluster "$CLUSTER" --data "data/$id" > "data/$id.log" 2>&1 &
  echo $! > "data/$id.pid"
  echo "started $id (pid $!)"
}

case "${1:-}" in
  start)
    make -s jar
    for id in $NODES; do start_node "$id"; done
    ;;
  status)
    for i in 1 2 3 4 5; do
      printf "n%s  " "$i"
      curl -s --max-time 1 "http://127.0.0.1:800$i/status" || echo "down"
    done
    ;;
  kill)
    kill -9 "$(cat "data/$2.pid")" && echo "killed $2"
    ;;
  restart)
    start_node "$2"
    ;;
  stop)
    for id in $NODES; do [ -f "data/$id.pid" ] && kill "$(cat "data/$id.pid")" 2>/dev/null || true; done
    echo "stopped"
    ;;
  *)
    sed -n '2,8p' "$0"
    exit 2
    ;;
esac
