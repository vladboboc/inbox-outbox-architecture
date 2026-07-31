#!/bin/bash
# Topic provisioning. auto.create.topics.enable is off on the brokers so that a typo in a topic
# name fails loudly instead of silently creating a 1-partition topic with default settings.
set -euo pipefail

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"

create() {
  local topic="$1" partitions="$2"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config min.insync.replicas=2
  echo "  ok: $topic ($partitions partitions, RF=3, min.isr=2)"
}

echo "waiting for all three brokers to register..."
for _ in $(seq 1 30); do
  count=$(kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" 2>/dev/null \
            | grep -c 'id: ' || true)
  [ "${count:-0}" -ge 3 ] && break
  sleep 2
done

echo "creating topics..."
create orders.v1            3
create orders.v1.cancelled  3
create orders.v1.DLT        3
create domain-events        3

echo
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
echo "topic provisioning complete"
