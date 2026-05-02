#!/usr/bin/env bash
# Generates a self-contained HTML load test report with Chart.js graphs.
# Called by load-test.sh — not meant to run standalone.

TS=$1; DIR=$2; OUT=$3
TOTAL=$4; COMPLETED=$5; FAILED=$6; CANCELLED=$7; TIMEDOUT=$8
THROUGHPUT=$9; MIN_LAT=${10}; P50_LAT=${11}; P95_LAT=${12}; P99_LAT=${13}; MAX_LAT=${14}; AVG_LAT=${15}
TOTAL_SECS=${16}; WAVES=${17}; WAVE_SIZE=${18}; DURATION=${19}
DIS2=${20}; LB=${21}

METRICS="$DIR/metrics_${TS}.csv"
JVM="$DIR/jvm_${TS}.csv"
MONGO="$DIR/mongo_${TS}.csv"
WAVE_CSV="$DIR/waves_${TS}.csv"
VENDOR="$DIR/vendor_api_${TS}.csv"
ENDPOINTS="$DIR/endpoints_${TS}.csv"

# Helper: CSV column to JS array
csv_col() { awk -F, "NR>1{print \$$2}" "$1" | paste -sd, -; }
csv_col_str() { awk -F, "NR>1{printf \"'%s',\",\$$2}" "$1"; }

# Extract DIS-1 time series
DIS1_TIMES=$(grep "digital-instrument-service-1" "$METRICS" | awk -F, '{printf "\"%s\",",$1}')
DIS1_CPU=$(grep "digital-instrument-service-1" "$METRICS" | awk -F, '{gsub(/%/,"",$3); printf "%.1f,",$3}')
DIS1_MEM=$(grep "digital-instrument-service-1" "$METRICS" | awk -F, '{split($4,a,"/"); gsub(/[A-Za-z ]/,"",a[1]); if(index($4,"GiB")) printf "%.0f,",a[1]*1024; else printf "%.0f,",a[1]+0}')

# DIS-2
DIS2_CPU=$(grep "digital-instrument-service-2" "$METRICS" | awk -F, '{gsub(/%/,"",$3); printf "%.1f,",$3}')
DIS2_MEM=$(grep "digital-instrument-service-2" "$METRICS" | awk -F, '{split($4,a,"/"); gsub(/[A-Za-z ]/,"",a[1]); if(index($4,"GiB")) printf "%.0f,",a[1]*1024; else printf "%.0f,",a[1]+0}')

# Kafka
KAFKA_CPU=$(grep "kafka-1" "$METRICS" | awk -F, '{gsub(/%/,"",$3); printf "%.1f,",$3}')

# MongoDB container
MONGO_CPU=$(grep "mongodb" "$METRICS" | awk -F, '{gsub(/%/,"",$3); printf "%.1f,",$3}')

# JVM heap (MiB)
JVM_TIMES=$(awk -F, 'NR>1{printf "\"%s\",",$1}' "$JVM")
JVM_HEAP=$(awk -F, 'NR>1{printf "%.0f,",$2/1048576}' "$JVM")
JVM_NONHEAP=$(awk -F, 'NR>1{printf "%.0f,",$3/1048576}' "$JVM")
JVM_THREADS=$(awk -F, 'NR>1{printf "%.0f,",$5}' "$JVM")

# MongoDB metrics
MONGO_TIMES=$(awk -F, 'NR>1{printf "\"%s\",",$1}' "$MONGO")
MONGO_POOL=$(awk -F, 'NR>1{printf "%.0f,",$2}' "$MONGO")
MONGO_CHECKED=$(awk -F, 'NR>1{printf "%.0f,",$3}' "$MONGO")
MONGO_CONN=$(awk -F, 'NR>1{printf "%.0f,",$5}' "$MONGO")
MONGO_OP_INSERT=$(awk -F, 'NR>1{printf "%.0f,",$7}' "$MONGO")
MONGO_OP_QUERY=$(awk -F, 'NR>1{printf "%.0f,",$8}' "$MONGO")
MONGO_OP_UPDATE=$(awk -F, 'NR>1{printf "%.0f,",$9}' "$MONGO")
MONGO_CACHE=$(awk -F, 'NR>1{printf "%.1f,",$11/1048576}' "$MONGO")

# Waves
WAVE_TIMES=$(awk -F, '{printf "\"%s\",",$1}' "$WAVE_CSV")
WAVE_SUBMITTED=$(awk -F, '{printf "%s,",$3}' "$WAVE_CSV")
WAVE_DONE=$(awk -F, '{printf "%s,",$4}' "$WAVE_CSV")

# Vendor API calls (final snapshot)
VENDOR_LABELS=""
VENDOR_COUNTS=""
while read line; do
    uri=$(echo "$line" | awk -F, '{print $2}' | sed 's|/api/v1/||')
    count=$(echo "$line" | awk -F, '{printf "%.0f",$3}')
    [ "$count" = "0" ] && continue
    VENDOR_LABELS="$VENDOR_LABELS\"$uri\","
    VENDOR_COUNTS="$VENDOR_COUNTS$count,"
done < <(grep "^final," "$VENDOR" 2>/dev/null)

cat > "$OUT" << 'HTMLEOF'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DIS Load Test Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0f172a;--sf:#1e293b;--bd:#334155;--tx:#e2e8f0;--dim:#94a3b8;--green:#10b981;--blue:#3b82f6;--amber:#f59e0b;--red:#ef4444;--purple:#a855f7}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--tx);line-height:1.6}
.container{max-width:1400px;margin:0 auto;padding:20px}
h1{font-size:1.8rem;margin-bottom:4px} h2{font-size:1.3rem;color:var(--blue);margin:30px 0 15px;border-bottom:1px solid var(--bd);padding-bottom:8px}
h3{font-size:1rem;color:var(--dim);margin:20px 0 10px}
.hero{background:linear-gradient(135deg,#1e3a5f,#0f172a);padding:30px;border-radius:12px;margin-bottom:30px;border:1px solid var(--bd)}
.hero .sub{color:var(--dim);font-size:.9rem}
.grid{display:grid;gap:20px} .grid-2{grid-template-columns:1fr 1fr} .grid-3{grid-template-columns:1fr 1fr 1fr} .grid-4{grid-template-columns:1fr 1fr 1fr 1fr}
.card{background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px}
.card .label{font-size:.75rem;color:var(--dim);text-transform:uppercase;letter-spacing:1px}
.card .value{font-size:1.8rem;font-weight:700;margin-top:4px}
.card .value.green{color:var(--green)} .card .value.blue{color:var(--blue)} .card .value.amber{color:var(--amber)} .card .value.red{color:var(--red)}
.chart-box{background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px;margin-bottom:20px}
.chart-box canvas{max-height:300px}
table{width:100%;border-collapse:collapse;font-size:.85rem;margin-bottom:20px}
th{background:var(--sf);color:var(--dim);text-align:left;padding:8px 12px;border-bottom:2px solid var(--bd);font-weight:600;text-transform:uppercase;font-size:.7rem;letter-spacing:1px}
td{padding:8px 12px;border-bottom:1px solid var(--bd)} tr:hover{background:rgba(59,130,246,.05)}
code{background:var(--sf);padding:2px 6px;border-radius:4px;font-size:.85rem}
.tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.7rem;font-weight:600}
.tag-g{background:rgba(16,185,129,.15);color:var(--green)} .tag-b{background:rgba(59,130,246,.15);color:var(--blue)}
.tag-a{background:rgba(245,158,11,.15);color:var(--amber)} .tag-r{background:rgba(239,68,68,.15);color:var(--red)}
.env-table td:first-child{color:var(--dim);width:200px}
@media(max-width:900px){.grid-2,.grid-3,.grid-4{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="container">
HTMLEOF

# Inject dynamic content
cat >> "$OUT" << EOF
<div class="hero">
  <h1>DIS Load &amp; Stability Test Report</h1>
  <p class="sub">$(date '+%Y-%m-%d %H:%M') &nbsp;|&nbsp; Duration: ${DURATION}min &nbsp;|&nbsp; ${WAVES} waves &times; ${WAVE_SIZE} flows &nbsp;|&nbsp; $(if [ "$DIS2" = "true" ]; then echo "2 DIS instances + Nginx LB"; else echo "Single DIS instance"; fi)</p>
</div>

<!-- ===== Highlight Cards ===== -->
<div class="grid grid-4" style="margin-bottom:30px">
  <div class="card"><div class="label">Total Flows</div><div class="value blue">$TOTAL</div></div>
  <div class="card"><div class="label">Completed</div><div class="value green">$COMPLETED</div></div>
  <div class="card"><div class="label">Throughput</div><div class="value amber">${THROUGHPUT} flows/sec</div></div>
  <div class="card"><div class="label">p50 Latency</div><div class="value">${P50_LAT}ms</div></div>
</div>
<div class="grid grid-4" style="margin-bottom:30px">
  <div class="card"><div class="label">Failed (flaky vendor)</div><div class="value$([ "$FAILED" -gt 0 ] && echo " red")">$FAILED</div></div>
  <div class="card"><div class="label">Cancelled (intentional)</div><div class="value">$CANCELLED</div></div>
  <div class="card"><div class="label">Timed Out</div><div class="value$([ "$TIMEDOUT" -gt 0 ] && echo " red")">$TIMEDOUT</div></div>
  <div class="card"><div class="label">Wall Time</div><div class="value">${TOTAL_SECS}s</div></div>
</div>

<h2>Environment</h2>
<table class="env-table">
  <tr><td>DIS</td><td>Spring Boot 4.0.5, Java 21, virtual threads$(if [ "$DIS2" = "true" ]; then echo " — <strong>2 instances</strong> (8087 + 8088)"; fi)$(if [ "$LB" = "true" ]; then echo " + Nginx LB (8090)"; fi)</td></tr>
  <tr><td>Kafka</td><td>3-broker cluster (Confluent 7.5.0), acks=all, min.insync.replicas=2</td></tr>
  <tr><td>MongoDB</td><td>7.0 standalone</td></tr>
  <tr><td>Mock Vendor</td><td>Enigio trace:original v3.3 simulation (200-600ms latency per call)</td></tr>
  <tr><td>Scenarios</td><td>40% happy path (PN/BoE/BoL) · 20% attachments · 10% additional docs · 10% cancel · 10% flaky · 10% vendor sync</td></tr>
</table>

<h2>Latency Distribution</h2>
<table>
  <tr><th>Min</th><th>p50</th><th>p95</th><th>p99</th><th>Max</th><th>Avg</th></tr>
  <tr><td>${MIN_LAT}ms</td><td><strong>${P50_LAT}ms</strong></td><td>${P95_LAT}ms</td><td>${P99_LAT}ms</td><td>${MAX_LAT}ms</td><td>${AVG_LAT}ms</td></tr>
</table>

<!-- ===== Charts ===== -->

<h2>Throughput Over Time</h2>
<div class="chart-box"><canvas id="waveChart"></canvas></div>

<h2>Container CPU</h2>
<div class="chart-box"><canvas id="cpuChart"></canvas></div>

<h2>Container Memory (MiB)</h2>
<div class="chart-box"><canvas id="memChart"></canvas></div>

<h2>JVM Heap &amp; Non-Heap (MiB)</h2>
<div class="chart-box"><canvas id="jvmChart"></canvas></div>

<h2>JVM Live Threads</h2>
<div class="chart-box"><canvas id="threadChart"></canvas></div>

<h2>MongoDB Connection Pool (DIS Actuator)</h2>
<div class="chart-box"><canvas id="mongoPoolChart"></canvas></div>

<h2>MongoDB Server — Connections &amp; WiredTiger Cache</h2>
<div class="grid grid-2">
  <div class="chart-box"><canvas id="mongoConnChart"></canvas></div>
  <div class="chart-box"><canvas id="mongoCacheChart"></canvas></div>
</div>

<h2>MongoDB OpCounters (cumulative)</h2>
<div class="chart-box"><canvas id="mongoOpsChart"></canvas></div>

<h2>Vendor API Calls</h2>
<div class="chart-box"><canvas id="vendorChart"></canvas></div>

<script>
Chart.defaults.color='#94a3b8';
Chart.defaults.borderColor='#334155';
Chart.defaults.font.size=11;
const line=(id,labels,datasets)=>new Chart(document.getElementById(id),{type:'line',data:{labels,datasets},options:{responsive:true,interaction:{intersect:false,mode:'index'},plugins:{legend:{position:'bottom'}},scales:{y:{beginAtZero:true}}}});
const bar=(id,labels,data,color)=>new Chart(document.getElementById(id),{type:'bar',data:{labels,datasets:[{data,backgroundColor:color||'rgba(59,130,246,.6)',borderColor:'rgba(59,130,246,1)',borderWidth:1}]},options:{responsive:true,indexAxis:'y',plugins:{legend:{display:false}}}});

// Throughput
line('waveChart',[$WAVE_TIMES],[
  {label:'Submitted',data:[$WAVE_SUBMITTED],borderColor:'#3b82f6',tension:.3},
  {label:'Completed',data:[$WAVE_DONE],borderColor:'#10b981',tension:.3}
]);

// CPU
line('cpuChart',[$DIS1_TIMES],[
  {label:'DIS-1 CPU%',data:[$DIS1_CPU],borderColor:'#3b82f6',tension:.3},
  {label:'DIS-2 CPU%',data:[$DIS2_CPU],borderColor:'#a855f7',tension:.3},
  {label:'Kafka-1 CPU%',data:[$KAFKA_CPU],borderColor:'#f59e0b',tension:.3},
  {label:'MongoDB CPU%',data:[$MONGO_CPU],borderColor:'#10b981',tension:.3}
]);

// Memory
line('memChart',[$DIS1_TIMES],[
  {label:'DIS-1 MiB',data:[$DIS1_MEM],borderColor:'#3b82f6',fill:true,backgroundColor:'rgba(59,130,246,.1)',tension:.3},
  {label:'DIS-2 MiB',data:[$DIS2_MEM],borderColor:'#a855f7',tension:.3}
]);

// JVM
line('jvmChart',[$JVM_TIMES],[
  {label:'Heap MiB',data:[$JVM_HEAP],borderColor:'#3b82f6',fill:true,backgroundColor:'rgba(59,130,246,.1)',tension:.3},
  {label:'Non-Heap MiB',data:[$JVM_NONHEAP],borderColor:'#f59e0b',tension:.3}
]);

// Threads
line('threadChart',[$JVM_TIMES],[
  {label:'Live Threads',data:[$JVM_THREADS],borderColor:'#a855f7',tension:.3}
]);

// MongoDB pool
line('mongoPoolChart',[$MONGO_TIMES],[
  {label:'Pool Size',data:[$MONGO_POOL],borderColor:'#3b82f6',tension:.3},
  {label:'Checked Out',data:[$MONGO_CHECKED],borderColor:'#ef4444',tension:.3}
]);

// MongoDB connections
line('mongoConnChart',[$MONGO_TIMES],[
  {label:'Current Connections',data:[$MONGO_CONN],borderColor:'#10b981',tension:.3}
]);

// WiredTiger cache
line('mongoCacheChart',[$MONGO_TIMES],[
  {label:'Cache MiB',data:[$MONGO_CACHE],borderColor:'#f59e0b',fill:true,backgroundColor:'rgba(245,158,11,.1)',tension:.3}
]);

// MongoDB ops
line('mongoOpsChart',[$MONGO_TIMES],[
  {label:'Insert',data:[$MONGO_OP_INSERT],borderColor:'#10b981',tension:.3},
  {label:'Query',data:[$MONGO_OP_QUERY],borderColor:'#3b82f6',tension:.3},
  {label:'Update',data:[$MONGO_OP_UPDATE],borderColor:'#f59e0b',tension:.3}
]);

// Vendor API
bar('vendorChart',[$VENDOR_LABELS],[$VENDOR_COUNTS]);
</script>
</div>
</body>
</html>
EOF

echo "HTML report generated: $OUT"
