#!/usr/bin/env bash
# ============================================================
# Infrastructure metrics collector
# Captures: Kafka, MongoDB, Docker containers, JVM, vendor calls
# Usage: source scripts/collect-metrics.sh
#        collect_metrics              # full snapshot
#        collect_metrics_brief        # one-line summary
# ============================================================

DIS_URL=${DIS_URL:-http://localhost:8087}
API_KEY=${API_KEY:-soak-test-key}

collect_metrics() {
  local label=${1:-"snapshot"}
  echo ""
  echo "=== METRICS [$label] $(date +%H:%M:%S) ==="

  # --- Docker containers ---
  echo "--- Containers ---"
  docker stats --no-stream --format "  {{.Name}}: CPU={{.CPUPerc}} MEM={{.MemUsage}} NET={{.NetIO}}" 2>/dev/null | grep -E "instrument|kafka|mongo|vendor" || echo "  (docker stats unavailable)"

  # --- Kafka consumer lag ---
  echo "--- Kafka Consumer Lag ---"
  docker exec infra-kafka-1-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 \
    --group digital-instrument-service-executor --describe 2>/dev/null | \
    awk 'NR>1 && $6 ~ /^[0-9]+$/ {lag+=$6; parts++} END {print "  Executor lag: " (lag+0) " across " (parts+0) " partitions"}' 2>/dev/null || echo "  (lag unavailable)"

  # --- Kafka topic offsets ---
  echo "--- Kafka Topics ---"
  docker exec infra-kafka-1-1 bash -c '
    for T in dis.instrument.commands dis.instrument.commands.replies dis.instrument.commands-dlt; do
      OFF=$(kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | awk -F: "{s+=\$3}END{print s}")
      echo "  $T: $OFF"
    done
    RETRY_TOTAL=0
    for T in dis.instrument.commands-retry-0 dis.instrument.commands-retry-1 dis.instrument.commands-retry-2 dis.instrument.commands-retry-3 dis.instrument.commands-retry-4 dis.instrument.commands-retry-5 dis.instrument.commands-retry-6 dis.instrument.commands-retry-7 dis.instrument.commands-retry-8; do
      OFF=$(kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | awk -F: "{s+=\$3}END{print s}")
      RETRY_TOTAL=$((RETRY_TOTAL + ${OFF:-0}))
    done
    echo "  retry topics total: $RETRY_TOTAL"
  ' 2>/dev/null || echo "  (topics unavailable)"

  # --- MongoDB ---
  echo "--- MongoDB ---"
  docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval '
    var flows = db.dis_instrument_flows.countDocuments({});
    var outbox = db.orchestrator_outbox.countDocuments({published: false});
    var events = db.orchestrator_processed_events.countDocuments({});
    var logs = db.orchestrator_step_log.countDocuments({});
    var orphans = db.dis_instrument_flows.countDocuments({claimedBy: {$ne: null}});
    var statuses = {};
    db.dis_instrument_flows.find({},{status:1,_id:0}).forEach(function(f){statuses[f.status]=(statuses[f.status]||0)+1});
    print("  Flows: " + flows + " | Outbox pending: " + outbox + " | Events: " + events + " | Logs: " + logs);
    print("  Orphaned claims: " + orphans);
    var statusLine = Object.keys(statuses).sort().map(function(k){return k+":"+statuses[k]}).join(" ");
    print("  Status: " + statusLine);
  ' 2>/dev/null || echo "  (MongoDB unavailable)"

  # --- JVM metrics (via actuator) ---
  echo "--- JVM (DIS) ---"
  local jvm=$(curl -sf "$DIS_URL/actuator/metrics/jvm.memory.used?tag=area:heap" -H "X-API-Key: $API_KEY" 2>/dev/null)
  local threads=$(curl -sf "$DIS_URL/actuator/metrics/jvm.threads.live" -H "X-API-Key: $API_KEY" 2>/dev/null)
  local gc=$(curl -sf "$DIS_URL/actuator/metrics/jvm.gc.pause" -H "X-API-Key: $API_KEY" 2>/dev/null)

  if [ -n "$jvm" ]; then
    HEAP=$(echo "$jvm" | python3 -c "import json,sys;m=json.load(sys.stdin)['measurements'];print(round(m[0]['value']/1048576))" 2>/dev/null)
    echo "  Heap used: ${HEAP:-?} MB"
  else
    echo "  Heap: (actuator unavailable)"
  fi

  if [ -n "$threads" ]; then
    THREAD_COUNT=$(echo "$threads" | python3 -c "import json,sys;m=json.load(sys.stdin)['measurements'];print(int(m[0]['value']))" 2>/dev/null)
    echo "  Live threads: ${THREAD_COUNT:-?}"
  else
    echo "  Threads: (actuator unavailable)"
  fi

  if [ -n "$gc" ]; then
    GC_COUNT=$(echo "$gc" | python3 -c "import json,sys;m=json.load(sys.stdin)['measurements'];print(int(m[0]['value']))" 2>/dev/null)
    GC_TIME=$(echo "$gc" | python3 -c "import json,sys;m=json.load(sys.stdin)['measurements'];print(round(m[1]['value']*1000))" 2>/dev/null)
    echo "  GC: count=${GC_COUNT:-?} totalMs=${GC_TIME:-?}"
  else
    echo "  GC: (actuator unavailable)"
  fi

  # --- Vendor calls ---
  echo "--- Mock Vendor ---"
  local DRAFTS=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Created draft" || echo 0)
  local VALIDATED=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Validated document" || echo 0)
  local SIGNERS=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Added.*required signatures" || echo 0)
  local SIGNING=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Signing emails sent" || echo 0)
  local SEALED=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Sealed envelope" || echo 0)
  local TRANSFERS=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Transfer initiated" || echo 0)
  local WEBHOOKS=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Webhook fired" || echo 0)
  echo "  Drafts:$DRAFTS Validated:$VALIDATED Signers:$SIGNERS Signing:$SIGNING Sealed:$SEALED Transfers:$TRANSFERS Webhooks:$WEBHOOKS"

  # --- DIS errors ---
  local ERRORS=$(docker logs infra-digital-instrument-service-1 2>&1 | grep -c "ERROR" || echo 0)
  echo "--- DIS Errors: $ERRORS ---"
}

# Brief one-liner for periodic snapshots
collect_metrics_brief() {
  local COMPLETED=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    'print(db.dis_instrument_flows.countDocuments({status:"COMPLETED"}))' 2>/dev/null || echo "?")
  local TOTAL=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    'print(db.dis_instrument_flows.countDocuments({}))' 2>/dev/null || echo "?")
  local OUTBOX=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    'print(db.orchestrator_outbox.countDocuments({published:false}))' 2>/dev/null || echo "?")
  local LAG=$(docker exec infra-kafka-1-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 \
    --group digital-instrument-service-executor --describe 2>/dev/null | \
    awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}' || echo "?")
  local HEAP=$(curl -sf "$DIS_URL/actuator/metrics/jvm.memory.used?tag=area:heap" -H "X-API-Key: $API_KEY" 2>/dev/null | \
    python3 -c "import json,sys;m=json.load(sys.stdin)['measurements'];print(round(m[0]['value']/1048576))" 2>/dev/null || echo "?")
  local THREADS=$(curl -sf "$DIS_URL/actuator/metrics/jvm.threads.live" -H "X-API-Key: $API_KEY" 2>/dev/null | \
    python3 -c "import json,sys;m=json.load(sys.stdin)['measurements'];print(int(m[0]['value']))" 2>/dev/null || echo "?")
  local ERRORS=$(docker logs infra-digital-instrument-service-1 2>&1 | grep -c "ERROR" || echo "?")

  echo "[$(date +%H:%M:%S)] flows=$COMPLETED/$TOTAL outbox=$OUTBOX lag=$LAG heap=${HEAP}MB threads=$THREADS errors=$ERRORS"
}
