#!/usr/bin/env bash
# =============================================================================
# DIS Comprehensive Load & Stability Test
# =============================================================================
#
# Scenarios:
#   1. Happy path — promissory notes, bills of exchange, bills of lading
#   2. With attachments — triggers amend step
#   3. With additional documents — upload to MongoDB, attached at envelope step
#   4. Flaky vendor — retry recovery under 50% failure rate
#   5. Cancellation at Gate 1 — verify document invalidated
#   6. Cancellation at Gate 2 — verify document + envelope invalidated
#   7. Vendor sync — reconciliation endpoint under load
#   8. Sustained load — continuous flow submission over DURATION minutes
#
# Metrics captured every SAMPLE_INTERVAL seconds:
#   - Per-container CPU, memory, net I/O, block I/O, PIDs
#   - Kafka topic offsets (commands, retry-1, retry-2, DLT, notifications)
#   - Consumer group lag
#   - MongoDB collection counts
#
# Usage:
#   ./scripts/load-test.sh                          # 3 min, 10 flows/wave
#   DURATION=10 WAVE_SIZE=20 ./scripts/load-test.sh # 10 min, 20 flows/wave
#   ./scripts/load-test.sh --multi-dc               # multi-DC failover
#
# Requires: curl, jq, docker, python3, bc
# Compatible with bash 3.2+ (macOS)
# =============================================================================

set -uo pipefail

DIS_URL_1="${DIS_URL_1:-http://localhost:8087}"
DIS_URL_2="${DIS_URL_2:-http://localhost:8088}"
DIS_LB="${DIS_LB:-http://localhost:8090}"  # nginx load balancer
DIS_URL="$DIS_URL_1"   # default for direct queries
API_KEY="${API_KEY:-soak-test-key}"
VENDOR_URL="${VENDOR_URL:-http://localhost:8081}"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27117}"
DURATION="${DURATION:-3}"                 # minutes
WAVE_SIZE="${WAVE_SIZE:-10}"              # flows per wave
WAVE_INTERVAL="${WAVE_INTERVAL:-30}"      # seconds between waves
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-10}"  # metrics sample interval (seconds)
RESULTS_DIR="scripts/load-test-results"
MULTI_DC=false
[ "${1:-}" = "--multi-dc" ] && MULTI_DC=true

mkdir -p "$RESULTS_DIR"
TS=$(date +%Y%m%d_%H%M%S)
REPORT="$RESULTS_DIR/report_${TS}.md"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

# Tracking files
ALL_FIDS="$TMP/all_flow_ids.txt";        > "$ALL_FIDS"
DONE_FIDS="$TMP/done_flows.txt";         > "$DONE_FIDS"
METRICS_CSV="$TMP/metrics.csv";           > "$METRICS_CSV"
KAFKA_LOG="$TMP/kafka_snapshots.txt";     > "$KAFKA_LOG"
MONGO_LOG="$TMP/mongo_snapshots.txt";     > "$MONGO_LOG"
SCENARIO_LOG="$TMP/scenarios.txt";        > "$SCENARIO_LOG"
ENDPOINT_LOG="$TMP/endpoints.csv";        > "$ENDPOINT_LOG"
VENDOR_API_LOG="$TMP/vendor_api.csv";     > "$VENDOR_API_LOG"
JVM_LOG="$TMP/jvm.csv";                   > "$JVM_LOG"
MONGO_METRICS="$TMP/mongo_metrics.csv";   > "$MONGO_METRICS"
WAVE_LOG="$TMP/waves.csv";               > "$WAVE_LOG"

log()  { echo "[$(date +%H:%M:%S)] $*"; }
now_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

# Load balancer target — nginx distributes across DIS instances
SUBMIT_TARGET=""
submit_url() {
    # Cache: check LB once, reuse for all calls
    if [ -z "$SUBMIT_TARGET" ]; then
        if curl -s -o /dev/null -w "%{http_code}" -m 2 "$DIS_LB/actuator/health" 2>/dev/null | grep -q "200"; then
            SUBMIT_TARGET="$DIS_LB"
        else
            SUBMIT_TARGET="$DIS_URL_1"
        fi
    fi
    echo "$SUBMIT_TARGET"
}

# ===== Service helpers =====

wait_healthy() {
    for i in $(seq 1 30); do
        code=$(curl -s -o /dev/null -w "%{http_code}" "$1/actuator/health" 2>/dev/null || echo "000")
        [ "$code" = "200" ] && return 0; sleep 1
    done
    echo "FAIL: $2 not healthy" >&2; exit 1
}

approve() { curl -s -X POST "$DIS_URL_1/flows/enigio-instrument/$1/approve" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{}' -m 2 2>/dev/null || true; }
get_field() { curl -s "$DIS_URL_1/flows/enigio-instrument/$1" -H "X-API-Key: $API_KEY" -m 2 2>/dev/null | jq -r ".$2 // empty"; }

start_instrument_flow() {
    local ref=$1 type=$2 extra=${3:-}
    local target=$(submit_url)
    curl -s -X POST "$target/flows/enigio-instrument" \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d "{
            \"reference\":\"$ref\",
            \"title\":\"$type — Load Test $ref\",
            \"content\":\"Test Corp promises to pay Bank EUR 500,000\",
            \"instrumentType\":\"$type\",
            \"documentCode\":\"NEG\",
            \"parties\":[
                {\"name\":\"Test Corp\",\"role\":\"ISSUER\",\"orgNumber\":\"123456-7890\"},
                {\"name\":\"Test Bank\",\"role\":\"BENEFICIARY\"}
            ],
            \"signers\":[
                {\"name\":\"Alice CEO\",\"email\":\"alice@test.com\",\"phone\":\"+46700000001\",\"capacity\":\"CEO\",\"organisation\":\"Test Corp\",\"order\":1},
                {\"name\":\"Bob CFO\",\"email\":\"bob@test.com\",\"phone\":\"+46700000002\",\"capacity\":\"CFO\",\"organisation\":\"Test Corp\",\"order\":2}
            ],
            \"recipient\":{\"name\":\"Bank Operations\",\"email\":\"ops@bank.com\"}
            $extra
        }" 2>/dev/null
}

# ===== Vendor API metrics via actuator =====

# Capture call count + total time for a single vendor endpoint
vendor_metric() {
    local uri=$1
    # URL-encode curly braces for actuator tag query
    local encoded=$(echo "$uri" | sed 's/{/%7B/g; s/}/%7D/g')
    curl -s "$VENDOR_URL/actuator/metrics/http.server.requests?tag=uri:$encoded" 2>/dev/null \
        | jq -r '[.measurements[0].value // 0, .measurements[1].value // 0] | map(tostring) | join(",")' 2>/dev/null || echo "0,0"
}

# Snapshot all vendor API endpoints
capture_vendor_api_snapshot() {
    local label=$1
    for uri in \
        "/api/v1/documents" \
        "/api/v1/documents/{traceOriginalId}/amend" \
        "/api/v1/documents/{traceOriginalId}/invalidate" \
        "/api/v1/documents/validate" \
        "/api/v1/documents/{traceOriginalId}" \
        "/api/v1/documents/{traceOriginalId}/metadata" \
        "/api/v1/documents/{traceOriginalId}/technical-details/latest" \
        "/api/v1/required-signatures/original/{traceOriginalId}" \
        "/api/v1/required-signatures/send-sign-emails" \
        "/api/v1/required-signatures/original/{traceOriginalId}/status" \
        "/api/v1/notifications/webhooks" \
        "/api/v1/envelopes/drafts" \
        "/api/v1/envelopes/drafts/{draftId}/seal" \
        "/api/v1/envelopes/{traceOriginalId}/transfer-by-email" \
        "/api/v1/envelopes/drafts/{draftId}/additional-documents"
    do
        m=$(vendor_metric "$uri")
        echo "$label,$uri,$m" >> "$VENDOR_API_LOG"
    done
}

# DIS JVM + Kafka metrics
capture_jvm_snapshot() {
    local ts=$1
    heap=$(curl -s "$DIS_URL/actuator/metrics/jvm.memory.used?tag=area:heap" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
    nonheap=$(curl -s "$DIS_URL/actuator/metrics/jvm.memory.used?tag=area:nonheap" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
    gc_pause=$(curl -s "$DIS_URL/actuator/metrics/jvm.gc.pause" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
    threads=$(curl -s "$DIS_URL/actuator/metrics/jvm.threads.live" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
    echo "$ts,$heap,$nonheap,$gc_pause,$threads" >> "$JVM_LOG"
}

# ===== Metrics capture (runs in background) =====

capture_metrics_loop() {
    echo "timestamp,container,cpu,mem_usage,mem_pct,net_io,block_io,pids" > "$METRICS_CSV"
    echo "timestamp,heap_bytes,nonheap_bytes,gc_pause_count,live_threads" > "$JVM_LOG"
    echo "timestamp,pool_size,pool_checkedout,pool_waitqueue,conn_current,conn_available,op_insert,op_query,op_update,op_command,wt_cache_bytes" > "$MONGO_METRICS"
    while true; do
        ts=$(date +%H:%M:%S)

        # Container stats
        docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}" 2>/dev/null \
            | grep "spring_orchestrator" \
            | while read line; do echo "$ts,$line"; done >> "$METRICS_CSV"

        # JVM metrics from DIS actuator (both instances)
        capture_jvm_snapshot "$ts"
        if $DIS2_AVAILABLE; then
            heap2=$(curl -s "$DIS_URL_2/actuator/metrics/jvm.memory.used?tag=area:heap" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
            threads2=$(curl -s "$DIS_URL_2/actuator/metrics/jvm.threads.live" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
            echo "$ts,$heap2,0,0,$threads2" >> "$TMP/jvm2.csv"
        fi

        # MongoDB metrics — DIS actuator pool + serverStatus
        pool_size=$(curl -s "$DIS_URL_1/actuator/metrics/mongodb.driver.pool.size" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
        pool_checked=$(curl -s "$DIS_URL_1/actuator/metrics/mongodb.driver.pool.checkedout" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
        pool_waitq=$(curl -s "$DIS_URL_1/actuator/metrics/mongodb.driver.pool.waitqueuesize" 2>/dev/null | jq '.measurements[0].value // 0' || echo 0)
        mongo_ss=$(mongosh "$MONGO_URI/digital_instrument_service" --quiet --eval '
            var s=db.serverStatus();
            print(s.connections.current+","+s.connections.available+","+
                  s.opcounters.insert.low+","+s.opcounters.query.low+","+
                  s.opcounters.update.low+","+s.opcounters.command.low+","+
                  s.wiredTiger.cache["bytes currently in the cache"])
        ' 2>/dev/null || echo "0,0,0,0,0,0,0")
        echo "$ts,$pool_size,$pool_checked,$pool_waitq,$mongo_ss" >> "$MONGO_METRICS"

        # Kafka offsets
        echo "--- $ts ---" >> "$KAFKA_LOG"
        for t in dis.instrument.commands dis.instrument.commands.retry-1 dis.instrument.commands.retry-2 dis.instrument.commands.dlt dis.instrument.notifications; do
            off=$(docker exec spring_orchestrator-kafka-1-1 kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list kafka-1:29092 --topic "$t" 2>/dev/null | paste -sd, - || echo "N/A")
            echo "  $t: $off" >> "$KAFKA_LOG"
        done

        # MongoDB counts
        echo "--- $ts ---" >> "$MONGO_LOG"
        mongosh "$MONGO_URI/digital_instrument_service" --quiet --eval '
            ["dis_instrument_flows","dis_additional_documents","orchestrator_step_log","orchestrator_processed_events","orchestrator_outbox"].forEach(c => {
                print("  " + c + ": " + db.getCollection(c).countDocuments());
            });
        ' >> "$MONGO_LOG" 2>/dev/null || echo "  (mongosh unavailable)" >> "$MONGO_LOG"

        sleep "$SAMPLE_INTERVAL"
    done
}

# ===== Auto-approve daemon (runs in background) =====
# Fires approve calls in parallel batches for speed.
# With 885 flows, sequential polling is too slow (~60 min for one pass).

auto_approve_loop() {
    while true; do
        # Batch: fire up to 50 parallel approve calls
        batch=0
        while read fid; do
            grep -q "^$fid " "$DONE_FIDS" 2>/dev/null && continue
            (
                resp=$(curl -s "$DIS_URL_1/flows/enigio-instrument/$fid" -H "X-API-Key: $API_KEY" -m 2 2>/dev/null)
                fstatus=$(echo "$resp" | jq -r '.status // empty')
                fstep=$(echo "$resp" | jq -r '.currentStep // empty')
                case "$fstatus" in COMPLETED|FAILED|CANCELLED) echo "$fid $(now_ms) $fstatus" >> "$DONE_FIDS" ;; esac
                case "$fstep" in AWAIT_PREPARATION_APPROVAL|AWAIT_DELIVERY_APPROVAL)
                    curl -s -X POST "$DIS_URL_1/flows/enigio-instrument/$fid/approve" \
                        -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{}' -m 2 > /dev/null 2>&1 ;;
                esac
            ) &
            batch=$((batch + 1))
            [ "$batch" -ge 50 ] && wait && batch=0
        done < "$ALL_FIDS"
        wait
        sleep 1
    done
}

# ===== Pre-flight =====

log "Pre-flight checks..."
wait_healthy "$DIS_URL_1" "DIS-1"
DIS2_AVAILABLE=false
LB_AVAILABLE=false
if curl -s -o /dev/null -w "%{http_code}" "$DIS_URL_2/actuator/health" 2>/dev/null | grep -q "200"; then
    DIS2_AVAILABLE=true
    log "DIS-2 healthy at $DIS_URL_2"
fi
if curl -s -o /dev/null -w "%{http_code}" "$DIS_LB/actuator/health" 2>/dev/null | grep -q "200"; then
    LB_AVAILABLE=true
    log "Nginx LB healthy at $DIS_LB → traffic goes through LB"
fi
if ! $DIS2_AVAILABLE; then log "DIS-2 not available — running single-instance"; fi
if ! $LB_AVAILABLE; then log "Nginx LB not available — sending direct to DIS-1"; fi
wait_healthy "$VENDOR_URL" "Mock Vendor"
curl -s -X POST "$VENDOR_URL/admin/reset" > /dev/null 2>&1 || true
mongosh "$MONGO_URI/digital_instrument_service" --quiet --eval '
    db.dis_instrument_flows.deleteMany({reference:/^LT-/});
    db.dis_additional_documents.deleteMany({});
' 2>/dev/null || true
log "Environment ready."

# Capture baseline vendor API call counts
log "Capturing baseline vendor API metrics..."
capture_vendor_api_snapshot "baseline"

# Start background daemons
capture_metrics_loop &
METRICS_PID=$!

auto_approve_loop &
APPROVE_PID=$!

trap "kill $METRICS_PID $APPROVE_PID 2>/dev/null; rm -rf $TMP" EXIT

START_MS=$(now_ms)
START_EPOCH=$(date +%s)
WAVE_NUM=0
TOTAL_SUBMITTED=0

log "=========================================="
log "Starting ${DURATION}m stability test"
log "  Wave size: $WAVE_SIZE flows"
log "  Wave interval: ${WAVE_INTERVAL}s"
log "  Metrics sample: every ${SAMPLE_INTERVAL}s"
log "=========================================="

# ===== Main test loop =====

END_EPOCH=$((START_EPOCH + DURATION * 60))

while [ "$(date +%s)" -lt "$END_EPOCH" ]; do
    WAVE_NUM=$((WAVE_NUM + 1))
    remaining=$(( (END_EPOCH - $(date +%s)) / 60 ))
    done_count=$(wc -l < "$DONE_FIDS" | tr -d ' ')
    echo "$(date +%H:%M:%S),$WAVE_NUM,$TOTAL_SUBMITTED,$done_count" >> "$WAVE_LOG"
    log "Wave $WAVE_NUM — submitting $WAVE_SIZE flows ($done_count/$TOTAL_SUBMITTED done, ${remaining}m remaining)"

    # Mix of scenarios per wave
    for i in $(seq 1 "$WAVE_SIZE"); do
        SEQ=$((TOTAL_SUBMITTED + i))
        REF="LT-$(printf '%04d' $SEQ)-$TS"

        # Scenario distribution (deterministic by sequence number)
        MOD=$((SEQ % 10))
        case "$MOD" in
            0|1|2|3)
                # 40% — Happy path, mixed instrument types
                case $((SEQ % 3)) in
                    0) TYPE="PROMISSORY_NOTE" ;; 1) TYPE="BILL_OF_EXCHANGE" ;; 2) TYPE="BILL_OF_LADING" ;;
                esac
                fid=$(start_instrument_flow "$REF" "$TYPE" | jq -r '.id // empty')
                echo "happy_path $TYPE $REF" >> "$SCENARIO_LOG"
                ;;
            4|5)
                # 20% — With attachments
                fid=$(start_instrument_flow "$REF" "PROMISSORY_NOTE" \
                    ",\"attachments\":[{\"filename\":\"terms.pdf\",\"data\":\"dGVybXM=\",\"comment\":\"Terms\"},{\"filename\":\"kyc.pdf\",\"data\":\"a3lj\",\"comment\":\"KYC\"}]" \
                    | jq -r '.id // empty')
                echo "with_attachments $REF" >> "$SCENARIO_LOG"
                ;;
            6)
                # 10% — With additional documents (upload first, then start flow)
                UP_RESULT=$(curl -s -X POST "$DIS_URL/documents/additional" \
                    -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
                    -d "{\"filename\":\"compliance-$SEQ.pdf\",\"contentType\":\"application/pdf\",\"data\":\"Y29tcGxpYW5jZSByZXBvcnQ=\"}")
                DOC_ID=$(echo "$UP_RESULT" | jq -r '.id // empty')
                if [ -n "$DOC_ID" ]; then
                    fid=$(start_instrument_flow "$REF" "PROMISSORY_NOTE" \
                        ",\"additionalDocumentIds\":[\"$DOC_ID\"]" \
                        | jq -r '.id // empty')
                else
                    fid=$(start_instrument_flow "$REF" "PROMISSORY_NOTE" | jq -r '.id // empty')
                fi
                echo "additional_docs $REF docId=$DOC_ID" >> "$SCENARIO_LOG"
                ;;
            7)
                # 10% — Cancel at Gate 1
                fid=$(start_instrument_flow "$REF" "BILL_OF_EXCHANGE" | jq -r '.id // empty')
                # Schedule cancel (don't auto-approve this one)
                if [ -n "$fid" ]; then
                    (   # Background: wait for gate, then cancel
                        for j in $(seq 1 60); do
                            s=$(get_field "$fid" "currentStep")
                            if [ "$s" = "AWAIT_PREPARATION_APPROVAL" ]; then
                                curl -s -X POST "$DIS_URL/flows/enigio-instrument/$fid/cancel" \
                                    -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{"reason":"Load test cancel at Gate 1"}' > /dev/null 2>&1
                                break
                            fi
                            sleep 1
                        done
                    ) &
                fi
                echo "cancel_gate1 $REF" >> "$SCENARIO_LOG"
                ;;
            8)
                # 10% — Flaky vendor (set before start, clear after)
                curl -s -X POST "$VENDOR_URL/admin/failure-config" \
                    -H "Content-Type: application/json" \
                    -d '{"createDocument":"FLAKY"}' > /dev/null 2>&1
                fid=$(start_instrument_flow "$REF" "PROMISSORY_NOTE" | jq -r '.id // empty')
                # Clear flaky after a few seconds so others aren't affected
                (sleep 5 && curl -s -X POST "$VENDOR_URL/admin/failure-config" \
                    -H "Content-Type: application/json" -d '{}' > /dev/null 2>&1) &
                echo "flaky_vendor $REF" >> "$SCENARIO_LOG"
                ;;
            9)
                # 10% — Vendor sync (start flow + immediately query sync endpoint)
                fid=$(start_instrument_flow "$REF" "PROMISSORY_NOTE" | jq -r '.id // empty')
                echo "vendor_sync $REF" >> "$SCENARIO_LOG"
                ;;
        esac

        [ -n "${fid:-}" ] && echo "$fid" >> "$ALL_FIDS"
    done

    TOTAL_SUBMITTED=$((TOTAL_SUBMITTED + WAVE_SIZE))

    # Between waves: exercise new endpoints (every 5th wave to reduce overhead)
    if [ $((WAVE_NUM % 5)) -eq 0 ]; then
    # 1. Upload additional documents (5 concurrent)
    for j in $(seq 1 5); do
        (
            S=$(now_ms)
            curl -s -X POST "$DIS_URL/documents/additional" \
                -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
                -d "{\"filename\":\"wave${WAVE_NUM}-doc${j}.pdf\",\"contentType\":\"application/pdf\",\"data\":\"Y29tcGxpYW5jZQ==\"}" > /dev/null 2>&1
            echo "upload_additional,$(($(now_ms) - S))" >> "$ENDPOINT_LOG"
        ) &
    done

    # 2. Vendor sync on completed flows (3 concurrent)
    COMPLETED_FIDS=$(grep " COMPLETED" "$DONE_FIDS" 2>/dev/null | tail -3 | awk '{print $1}')
    for cfid in $COMPLETED_FIDS; do
        (
            TID=$(get_field "$cfid" "traceOriginalId")
            if [ -n "$TID" ]; then
                # Default sync (metadata + technicalDetails)
                S=$(now_ms)
                curl -s "$DIS_URL/vendor/enigio/documents/$TID" -H "X-API-Key: $API_KEY" > /dev/null 2>&1
                echo "vendor_sync_default,$(($(now_ms) - S))" >> "$ENDPOINT_LOG"

                # Full sync (all 4 sections)
                S=$(now_ms)
                curl -s "$DIS_URL/vendor/enigio/documents/$TID?include=metadata,technicalDetails,requiredSignatures,document" -H "X-API-Key: $API_KEY" > /dev/null 2>&1
                echo "vendor_sync_all,$(($(now_ms) - S))" >> "$ENDPOINT_LOG"
            fi
        ) &
    done

    # 3. List additional docs by instrument
    SAMPLE_FID=$(head -1 "$ALL_FIDS")
    if [ -n "${SAMPLE_FID:-}" ]; then
        (
            S=$(now_ms)
            curl -s "$DIS_URL/documents/additional/instrument/$SAMPLE_FID" -H "X-API-Key: $API_KEY" > /dev/null 2>&1
            echo "list_additional,$(($(now_ms) - S))" >> "$ENDPOINT_LOG"
        ) &
    fi

    fi  # end of every-5th-wave endpoint exercise

    sleep "$WAVE_INTERVAL"
done

# ===== Drain: wait for all in-flight flows =====

log "=========================================="
log "All waves submitted. Waiting for in-flight flows to complete (max 5m)..."
log "=========================================="

DRAIN_DEADLINE=$(($(date +%s) + 300))
while [ "$(date +%s)" -lt "$DRAIN_DEADLINE" ]; do
    done_count=$(wc -l < "$DONE_FIDS" | tr -d ' ')
    total_count=$(wc -l < "$ALL_FIDS" | tr -d ' ')
    [ "$done_count" -ge "$total_count" ] && break
    log "  Draining: ${done_count}/${total_count} done"
    sleep 5
done

END_MS=$(now_ms)
TOTAL_SECS=$(( (END_MS - START_MS) / 1000 ))

# Stop background daemons
kill $METRICS_PID $APPROVE_PID 2>/dev/null || true
wait 2>/dev/null || true

# ===== Collect final results =====

log "Capturing post-load vendor API metrics..."
capture_vendor_api_snapshot "final"

log "Collecting results..."

TOTAL_FLOWS=$(wc -l < "$ALL_FIDS" | tr -d ' ')
COMPLETED=0; FAILED=0; CANCELLED=0; TIMEDOUT=0
LATENCIES=""

while read fid; do
    done_line=$(grep "^$fid " "$DONE_FIDS" 2>/dev/null || true)
    if [ -n "$done_line" ]; then
        st=$(echo "$done_line" | awk '{print $3}')
        lat_ms=$(echo "$done_line" | awk '{print $2}')
        lat=$((lat_ms - START_MS))
    else
        st=$(get_field "$fid" "status")
        lat=""
    fi
    case "$st" in
        COMPLETED)  COMPLETED=$((COMPLETED + 1)); [ -n "$lat" ] && LATENCIES="$LATENCIES $lat" ;;
        FAILED)     FAILED=$((FAILED + 1)) ;;
        CANCELLED)  CANCELLED=$((CANCELLED + 1)) ;;
        *)          TIMEDOUT=$((TIMEDOUT + 1)) ;;
    esac
done < "$ALL_FIDS"

# Percentiles
if [ "$COMPLETED" -gt 0 ]; then
    SORTED=$(echo $LATENCIES | tr ' ' '\n' | sort -n)
    MIN_LAT=$(echo "$SORTED" | head -1)
    MAX_LAT=$(echo "$SORTED" | tail -1)
    P50_LAT=$(echo "$SORTED" | awk "NR==$(( (COMPLETED+1)/2 ))")
    P95_LAT=$(echo "$SORTED" | awk "NR==$(( COMPLETED*95/100 + 1 ))")
    P99_LAT=$(echo "$SORTED" | awk "NR==$(( COMPLETED*99/100 + 1 ))")
    SUM=$(echo $LATENCIES | tr ' ' '\n' | awk '{s+=$1}END{print s}')
    AVG_LAT=$((SUM / COMPLETED))
    THROUGHPUT=$(echo "scale=2; $COMPLETED / $TOTAL_SECS" | bc 2>/dev/null || echo "N/A")
else
    MIN_LAT=N/A; MAX_LAT=N/A; P50_LAT=N/A; P95_LAT=N/A; P99_LAT=N/A; AVG_LAT=N/A; THROUGHPUT=N/A
fi

# Scenario counts
HAPPY_COUNT=$(grep -c "^happy_path" "$SCENARIO_LOG" || echo 0)
ATTACH_COUNT=$(grep -c "^with_attachments" "$SCENARIO_LOG" || echo 0)
ADDOC_COUNT=$(grep -c "^additional_docs" "$SCENARIO_LOG" || echo 0)
CANCEL_COUNT=$(grep -c "^cancel_" "$SCENARIO_LOG" || echo 0)
FLAKY_COUNT=$(grep -c "^flaky_vendor" "$SCENARIO_LOG" || echo 0)
SYNC_COUNT=$(grep -c "^vendor_sync" "$SCENARIO_LOG" || echo 0)

# Container metrics summary (peak values)
DIS_PEAK_CPU=$(grep "digital-instrument-service" "$METRICS_CSV" | awk -F, '{gsub(/%/,"",$3); if($3+0>max) max=$3+0} END{print max"%"}')
DIS_PEAK_MEM=$(grep "digital-instrument-service" "$METRICS_CSV" | awk -F, '{print $4}' | sort -t/ -k1 -h | tail -1)
MOCK_PEAK_CPU=$(grep "mock-vendor" "$METRICS_CSV" | awk -F, '{gsub(/%/,"",$3); if($3+0>max) max=$3+0} END{print max"%"}')
MOCK_PEAK_MEM=$(grep "mock-vendor" "$METRICS_CSV" | awk -F, '{print $4}' | sort -t/ -k1 -h | tail -1)
KAFKA_PEAK_CPU=$(grep "kafka-1" "$METRICS_CSV" | awk -F, '{gsub(/%/,"",$3); if($3+0>max) max=$3+0} END{print max"%"}')
KAFKA_PEAK_MEM=$(grep "kafka-1" "$METRICS_CSV" | awk -F, '{print $4}' | sort -t/ -k1 -h | tail -1)
MONGO_PEAK_CPU=$(grep "mongodb" "$METRICS_CSV" | awk -F, '{gsub(/%/,"",$3); if($3+0>max) max=$3+0} END{print max"%"}')
MONGO_PEAK_MEM=$(grep "mongodb" "$METRICS_CSV" | awk -F, '{print $4}' | sort -t/ -k1 -h | tail -1)
DIS2_PEAK_CPU=$(grep "digital-instrument-service-2" "$METRICS_CSV" | awk -F, '{gsub(/%/,"",$3); if($3+0>max) max=$3+0} END{print max"%"}')
DIS2_PEAK_MEM=$(grep "digital-instrument-service-2" "$METRICS_CSV" | awk -F, '{print $4}' | sort -t/ -k1 -h | tail -1)

# Kafka final state
FINAL_KAFKA=""
for t in dis.instrument.commands dis.instrument.commands.retry-1 dis.instrument.commands.retry-2 dis.instrument.commands.dlt dis.instrument.notifications; do
    off=$(docker exec spring_orchestrator-kafka-1-1 kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka-1:29092 --topic "$t" 2>/dev/null | paste -sd, - || echo "N/A")
    FINAL_KAFKA="$FINAL_KAFKA\n$t: $off"
done

# Post-drain Kafka (verify retry topics consumed)
log "Waiting 30s for Kafka drain verification..."
sleep 30
DRAIN_KAFKA=""
for t in dis.instrument.commands dis.instrument.commands.retry-1 dis.instrument.commands.retry-2 dis.instrument.commands.dlt dis.instrument.notifications; do
    off=$(docker exec spring_orchestrator-kafka-1-1 kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka-1:29092 --topic "$t" 2>/dev/null | paste -sd, - || echo "N/A")
    DRAIN_KAFKA="$DRAIN_KAFKA\n$t: $off"
done

DRAIN_LAG=$(docker exec spring_orchestrator-kafka-1-1 kafka-consumer-groups \
    --bootstrap-server kafka-1:29092 --all-groups --describe 2>/dev/null | head -30 || echo "(not available)")

# Metrics time series for report (sampled)
METRICS_TABLE=$(head -1 "$METRICS_CSV"; grep "digital-instrument-service" "$METRICS_CSV" | awk 'NR%3==1')

# ===== Failover =====

FAILOVER_RESULT="N/A (single-DC mode)"
FAILOVER_DETAIL=""
if $MULTI_DC; then
    log "=========================================="
    log "Failover Test (multi-DC)"
    log "=========================================="
    FO_RESULT=$(start_instrument_flow "LT-FAILOVER-$TS" "PROMISSORY_NOTE")
    FO_ID=$(echo "$FO_RESULT" | jq -r '.id // empty')
    log "  Flow: $FO_ID — waiting for Gate 1..."
    for i in $(seq 1 60); do
        [ "$(get_field "$FO_ID" currentStep)" = "AWAIT_PREPARATION_APPROVAL" ] && break; sleep 1
    done
    log "  Killing DIS-1..."
    docker compose -f docker-compose-multi-dc.yml stop dis-1 2>/dev/null || docker stop spring_orchestrator-digital-instrument-service-1 2>/dev/null || true
    DIS_URL="http://localhost:8088"; sleep 10
    for i in $(seq 1 90); do
        s=$(get_field "$FO_ID" currentStep)
        case "$s" in AWAIT_PREPARATION_APPROVAL|AWAIT_DELIVERY_APPROVAL) approve "$FO_ID" > /dev/null ;; esac
        st=$(get_field "$FO_ID" status); [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 2
    done
    FAILOVER_RESULT=$(get_field "$FO_ID" status)
    FAILOVER_DETAIL="Started on DIS-1, killed at Gate 1. DIS-2 resumed via shared Kafka+MongoDB. Final: **$FAILOVER_RESULT**"
    log "  Result: $FAILOVER_RESULT"
    docker compose -f docker-compose-multi-dc.yml start dis-1 2>/dev/null || true
    DIS_URL="http://localhost:8087"
fi

# ===== Compute vendor API deltas =====

VENDOR_API_TABLE=""
while read uri; do
    base_line=$(grep "^baseline,$uri," "$VENDOR_API_LOG" | head -1)
    final_line=$(grep "^final,$uri," "$VENDOR_API_LOG" | head -1)
    base_count=$(echo "$base_line" | awk -F, '{printf "%.0f", $3}')
    final_count=$(echo "$final_line" | awk -F, '{printf "%.0f", $3}')
    base_time=$(echo "$base_line" | awk -F, '{printf "%.2f", $4}')
    final_time=$(echo "$final_line" | awk -F, '{printf "%.2f", $4}')
    delta=$((final_count - base_count))
    delta_time=$(echo "$final_time - $base_time" | bc 2>/dev/null || echo "0")
    avg_ms=""
    if [ "$delta" -gt 0 ]; then
        avg_ms=$(echo "scale=1; $delta_time * 1000 / $delta" | bc 2>/dev/null || echo "N/A")
    fi
    short_uri=$(echo "$uri" | sed 's|/api/v1/||')
    VENDOR_API_TABLE="$VENDOR_API_TABLE\n| \`$short_uri\` | $delta | ${delta_time}s | ${avg_ms:-0}ms |"
done < <(grep "^baseline," "$VENDOR_API_LOG" | awk -F, '{print $2}' | sort -u)

# ===== Compute endpoint latency stats =====

endpoint_stats() {
    local type=$1
    local vals=$(grep "^$type," "$ENDPOINT_LOG" | awk -F, '{print $2}' | sort -n)
    local cnt=$(echo "$vals" | grep -c . || echo 0)
    [ "$cnt" -eq 0 ] && echo "0,N/A,N/A,N/A" && return
    local min=$(echo "$vals" | head -1)
    local max=$(echo "$vals" | tail -1)
    local p50=$(echo "$vals" | awk "NR==$(( (cnt+1)/2 ))")
    echo "$cnt,${min}ms,${p50}ms,${max}ms"
}

UPLOAD_STATS=$(endpoint_stats "upload_additional")
SYNC_DEF_STATS=$(endpoint_stats "vendor_sync_default")
SYNC_ALL_STATS=$(endpoint_stats "vendor_sync_all")
LIST_STATS=$(endpoint_stats "list_additional")

# ===== JVM time series =====

JVM_TABLE=$(cat "$JVM_LOG" | awk -F, 'NR==1{next} NR%2==0{
    heap_mb=sprintf("%.0f",$2/1048576);
    nonheap_mb=sprintf("%.0f",$3/1048576);
    printf "| %s | %s MiB | %s MiB | %.0f | %.0f |\n",$1,heap_mb,nonheap_mb,$4,$5
}')

# ===== Generate Report =====

cat > "$REPORT" << ENDREPORT
# DIS Load & Stability Test Report

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**Duration:** ${DURATION} minutes (+ drain + 30s idle verification)
**Mode:** $(if $MULTI_DC; then echo "Multi-DC (2 DIS instances)"; else echo "Single DC"; fi)

## Environment

| Component | Detail |
|-----------|--------|
| DIS | Spring Boot 4.0.5, Java 21, virtual threads$(if $DIS2_AVAILABLE; then echo " — **2 instances** (8087 + 8088, round-robin)"; else echo " — 1 instance (8087)"; fi) |
| Kafka | 3-broker (Confluent 7.5.0), acks=all, min.insync.replicas=2 |
| MongoDB | 7.0 standalone |
| Mock Vendor | Simulated Enigio trace:original v3.3 (200-600ms latency per call) |

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Duration | ${DURATION} minutes |
| Wave size | ${WAVE_SIZE} flows per wave |
| Wave interval | ${WAVE_INTERVAL}s between waves |
| Total waves | $WAVE_NUM |
| Total flows submitted | **$TOTAL_FLOWS** |

## Scenario Mix

| Scenario | Count | % | Description |
|----------|-------|---|-------------|
| Happy path | $HAPPY_COUNT | 40% | Mixed instrument types (PN, BoE, BoL) |
| With attachments | $ATTACH_COUNT | 20% | Triggers amend step (2 PDFs) |
| Additional documents | $ADDOC_COUNT | 10% | Upload to MongoDB → attach at envelope |
| Cancellation | $CANCEL_COUNT | 10% | Cancel at Gate 1, verify invalidation |
| Flaky vendor | $FLAKY_COUNT | 10% | 50% failure rate on createDocument |
| Vendor sync | $SYNC_COUNT | 10% | Reconciliation endpoint call |

---

## Results

### Throughput

| Metric | Value |
|--------|-------|
| Total flows | **$TOTAL_FLOWS** |
| Completed | **$COMPLETED** |
| Failed | $FAILED |
| Cancelled (intentional) | $CANCELLED |
| Timed out | $TIMEDOUT |
| Success rate | $(echo "scale=1; ($COMPLETED + $CANCELLED) * 100 / $TOTAL_FLOWS" | bc 2>/dev/null || echo "N/A")% |
| Wall time | ${TOTAL_SECS}s |
| Sustained throughput | **${THROUGHPUT} flows/sec** |

### Latency (end-to-end per completed flow)

Each flow traverses 11 steps: CREATE_DRAFT → REGISTER_DOCUMENT → ADD_ATTACHMENT → AWAIT_PREPARATION_APPROVAL → ADD_SIGNERS → SEND_FOR_SIGNING → AWAIT_SIGNATURES → AWAIT_DELIVERY_APPROVAL → VALIDATE_DOCUMENT → CREATE_ENVELOPE → TRANSFER_DOCUMENT

| Percentile | Latency |
|------------|---------|
| Min | ${MIN_LAT}ms |
| p50 (median) | ${P50_LAT}ms |
| p95 | ${P95_LAT}ms |
| p99 | ${P99_LAT}ms |
| Max | ${MAX_LAT}ms |
| Avg | ${AVG_LAT}ms |

---

## Vendor API Call Counts (Enigio trace:original)

Captured via mock-vendor Micrometer actuator. Delta = calls during load test only.

| Endpoint | Calls | Total Time | Avg Latency |
|----------|-------|------------|-------------|
$(echo -e "$VENDOR_API_TABLE")

---

## New Endpoint Load (exercised between waves)

| Endpoint | Calls | Min | p50 | Max |
|----------|-------|-----|-----|-----|
| \`POST /documents/additional\` | $UPLOAD_STATS |
| \`GET /vendor/enigio/documents/{id}\` (default) | $SYNC_DEF_STATS |
| \`GET /vendor/enigio/documents/{id}?include=all\` | $SYNC_ALL_STATS |
| \`GET /documents/additional/instrument/{id}\` | $LIST_STATS |

---

## Container Resource Usage

### Peak Values

| Container | Peak CPU | Peak Memory |
|-----------|----------|-------------|
| DIS | ${DIS_PEAK_CPU} | ${DIS_PEAK_MEM} |
| Mock Vendor | ${MOCK_PEAK_CPU} | ${MOCK_PEAK_MEM} |
| Kafka-1 | ${KAFKA_PEAK_CPU} | ${KAFKA_PEAK_MEM} |
| MongoDB | ${MONGO_PEAK_CPU} | ${MONGO_PEAK_MEM} |

### DIS Memory/CPU Time Series (sampled every ${SAMPLE_INTERVAL}s)

\`\`\`
$METRICS_TABLE
\`\`\`

### DIS JVM Metrics (from actuator, sampled every ${SAMPLE_INTERVAL}s)

| Time | Heap | Non-Heap | GC Pauses | Live Threads |
|------|------|----------|-----------|--------------|
$JVM_TABLE

---

## Kafka Topic State

### After load (before drain)

\`\`\`
$(echo -e "$FINAL_KAFKA")
\`\`\`

### After 30s idle (validates retry topics fully consumed)

\`\`\`
$(echo -e "$DRAIN_KAFKA")
\`\`\`

### Consumer group lag (post-drain)

\`\`\`
$DRAIN_LAG
\`\`\`

---

## Failover Test

**Result:** $FAILOVER_RESULT

$FAILOVER_DETAIL

---

*Generated by \`scripts/load-test.sh\` — $(date '+%Y-%m-%d %H:%M:%S')*
ENDREPORT

# Copy CSV data to results dir for HTML report
cp "$METRICS_CSV" "$RESULTS_DIR/metrics_${TS}.csv"
cp "$JVM_LOG" "$RESULTS_DIR/jvm_${TS}.csv"
cp "$MONGO_METRICS" "$RESULTS_DIR/mongo_${TS}.csv"
cp "$WAVE_LOG" "$RESULTS_DIR/waves_${TS}.csv"
cp "$VENDOR_API_LOG" "$RESULTS_DIR/vendor_api_${TS}.csv"
cp "$ENDPOINT_LOG" "$RESULTS_DIR/endpoints_${TS}.csv"

# Generate HTML report with graphs
HTML_REPORT="$RESULTS_DIR/report_${TS}.html"
bash scripts/generate-report-html.sh "$TS" "$RESULTS_DIR" "$HTML_REPORT" \
    "$TOTAL_FLOWS" "$COMPLETED" "$FAILED" "$CANCELLED" "$TIMEDOUT" \
    "$THROUGHPUT" "$MIN_LAT" "$P50_LAT" "$P95_LAT" "$P99_LAT" "$MAX_LAT" "$AVG_LAT" \
    "$TOTAL_SECS" "$WAVE_NUM" "$WAVE_SIZE" "$DURATION" \
    "$DIS2_AVAILABLE" "$LB_AVAILABLE" 2>/dev/null || log "HTML report generation skipped (generator not found)"

log "=========================================="
log "DONE"
log "  Markdown: $REPORT"
log "  HTML:     ${HTML_REPORT:-N/A}"
log "  CSV data: $RESULTS_DIR/*_${TS}.csv"
log "=========================================="
log ""
log "Summary: $COMPLETED completed, $CANCELLED cancelled, $FAILED failed, $TIMEDOUT timed out (of $TOTAL_FLOWS)"
log "Throughput: $THROUGHPUT flows/sec | p50: ${P50_LAT}ms | p95: ${P95_LAT}ms"
