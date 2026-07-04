#!/usr/bin/env bash
# Continuous monitor of retry + DLT flow across both clusters (fast: 1 describe call/cluster).
# A failing record should move main -> retry-0..N -> dlt, never wedge a partition.
# Per family per cluster it prints: main-topic LAG, total retry-topic messages, total DLT messages,
# so you can watch records progress through the retry chain into DLT (vs. lag stuck on the main topic).
#
# Usage: INTERVAL=15 bash scripts/monitor-dlt-retry.sh
set -uo pipefail
INTERVAL=${INTERVAL:-15}
CLUSTERS=("infra-kafka-a-1:kafka-a:29092:dc-a" "infra-kafka-b-1:kafka-b:29093:dc-b")

echo "[monitor] retry/DLT flow — interval=${INTERVAL}s   (LEO=log-end-offset=total msgs)"
echo "[monitor] per family: main_lag | retry_msgs(0..8) | dlt_msgs"
while true; do
  ts=$(date +%H:%M:%S)
  for spec in "${CLUSTERS[@]}"; do
    cont=${spec%%:*}; rest=${spec#*:}; boot=${rest%:*}; dc=${spec##*:}
    docker exec "$cont" kafka-consumer-groups --bootstrap-server "$boot" --all-groups --describe 2>/dev/null \
      | awk -v ts="$ts" -v dc="$dc" '
        $2 ~ /instrument\.commands/ && $6 ~ /^[0-9]+$/ {
          topic=$2; leo=$5+0; lag=$6+0;
          # derive family + role
          fam=topic; role="main";
          if (topic ~ /-dlt$/)      { sub(/-dlt$/,"",fam); role="dlt" }
          else if (topic ~ /-retry-[0-9]+$/) { sub(/-retry-[0-9]+$/,"",fam); role="retry" }
          else if (topic ~ /\.replies/) { next }
          if (role=="main")  mainlag[fam]+=lag;
          if (role=="retry") retry[fam]+=leo;
          if (role=="dlt")   dlt[fam]+=leo;
          fams[fam]=1;
        }
        END{
          for (f in fams) {
            ml=mainlag[f]+0; rt=retry[f]+0; dl=dlt[f]+0;
            if (ml>0 || rt>0 || dl>0)
              printf "[%s][%s] %-30s main_lag=%-5d retry=%-4d dlt=%-4d\n", ts, dc, f, ml, rt, dl;
          }
        }'
  done
  sleep "$INTERVAL"
done
