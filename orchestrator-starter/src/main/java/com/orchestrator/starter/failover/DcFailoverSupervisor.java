package com.orchestrator.starter.failover;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.UUID;

/**
 * DC failover supervisor — monitors active DC health and triggers
 * consumer/producer swap to standby DC on failure.
 *
 * State machine: HEALTHY → DEGRADED → FAILING_OVER → COOLDOWN → HEALTHY
 *
 * The supervisor does NOT stop/rebuild consumers. It signals the
 * {@link DcAwareKafkaManager} to swap the active consumer/producer pair.
 * Both pairs are pre-created (warm standby) — swap is ~100ms.
 */
@Slf4j
public class DcFailoverSupervisor {

    private final DcHealthProbe probe;
    private final DcAwareKafkaManager kafkaManager;
    private final MongoTemplate mongoTemplate;
    private final OrchestratorProperties.FailoverConfig config;
    private final String consumerId;

    @Getter private volatile DcState state = DcState.HEALTHY;
    @Getter private volatile String activeDc;
    private volatile String standbyDc;
    private int consecutiveFailures = 0;
    private int consecutiveSuccesses = 0;
    private Instant lastTransitionAt = Instant.EPOCH;

    public DcFailoverSupervisor(DcHealthProbe probe,
                                 DcAwareKafkaManager kafkaManager,
                                 MongoTemplate mongoTemplate,
                                 OrchestratorProperties.FailoverConfig config) {
        this.probe = probe;
        this.kafkaManager = kafkaManager;
        this.mongoTemplate = mongoTemplate;
        this.config = config;
        this.activeDc = config.getActiveDc();
        this.consumerId = System.getenv("HOSTNAME") != null
                ? System.getenv("HOSTNAME")
                : "supervisor-" + UUID.randomUUID().toString().substring(0, 8);

        // Determine standby DC
        this.standbyDc = config.getDcs().keySet().stream()
                .filter(dc -> !dc.equals(activeDc))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Failover requires at least 2 DCs. Configured: " + config.getDcs().keySet()));

        log.info("[Failover] Supervisor started: active={}, standby={}, policy={}, " +
                        "degraded={}failures, failover={}failures, dwell={}s",
                activeDc, standbyDc, config.getReplicationPolicy(),
                config.getDegradedThreshold(), config.getFailoverThreshold(),
                config.getDwellTimeSeconds());
    }

    /**
     * Periodic health probe — runs every probeIntervalMs.
     * Evaluates the state machine and triggers failover when needed.
     */
    @Scheduled(fixedDelayString = "${orchestrator.failover.probe-interval-ms:5000}")
    public void probeAndEvaluate() {
        boolean healthy = probe.probe(activeDc);

        if (healthy) {
            consecutiveSuccesses++;
            consecutiveFailures = 0;

            if (state == DcState.DEGRADED && consecutiveSuccesses >= config.getDegradedThreshold()) {
                transition(DcState.HEALTHY, consecutiveSuccesses + " consecutive successes — recovered");
            }
            if (state == DcState.COOLDOWN
                    && Instant.now().isAfter(lastTransitionAt.plusSeconds(config.getDwellTimeSeconds()))) {
                transition(DcState.HEALTHY, "dwell period elapsed, DC stable");
            }
        } else {
            consecutiveFailures++;
            consecutiveSuccesses = 0;

            if (state == DcState.HEALTHY && consecutiveFailures >= config.getDegradedThreshold()) {
                transition(DcState.DEGRADED, consecutiveFailures + " consecutive probe failures");
            }

            if (state == DcState.DEGRADED && consecutiveFailures >= config.getFailoverThreshold()) {
                if (Instant.now().isBefore(lastTransitionAt.plusSeconds(config.getDwellTimeSeconds()))) {
                    log.warn("[Failover] Would fail over but within dwell window ({} failures)",
                            consecutiveFailures);
                } else {
                    // Verify standby is reachable before failing over
                    if (probe.probe(standbyDc)) {
                        executeFailover();
                    } else {
                        log.error("[Failover] Both DCs unhealthy! active={} standby={} — staying on {}",
                                activeDc, standbyDc, activeDc);
                    }
                }
            }
        }
    }

    private void executeFailover() {
        transition(DcState.FAILING_OVER, "failing over from " + activeDc + " to " + standbyDc);

        try {
            String fromDc = activeDc;
            String toDc = standbyDc;

            kafkaManager.switchActiveDc(toDc);

            // Swap active/standby
            activeDc = toDc;
            standbyDc = fromDc;
            consecutiveFailures = 0;
            consecutiveSuccesses = 0;

            recordTransition(fromDc, toDc, "failover: " + fromDc + " unhealthy");
            transition(DcState.COOLDOWN, "failover complete: now active on " + toDc);

            log.info("[Failover] Successfully failed over: {} → {}", fromDc, toDc);
        } catch (Exception e) {
            log.error("[Failover] Failover failed! Staying on {}: {}", activeDc, e.getMessage(), e);
            transition(DcState.DEGRADED, "failover failed: " + e.getMessage());
        }
    }

    private void transition(DcState newState, String reason) {
        DcState previous = this.state;
        this.state = newState;
        this.lastTransitionAt = Instant.now();
        log.info("[Failover] State: {} → {} ({})", previous, newState, reason);
    }

    private void recordTransition(String fromDc, String toDc, String reason) {
        try {
            mongoTemplate.save(DcTransitionEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .consumerId(consumerId)
                    .fromDc(fromDc)
                    .toDc(toDc)
                    .reason(reason)
                    .previousState(DcState.FAILING_OVER)
                    .newState(DcState.COOLDOWN)
                    .consecutiveFailures(consecutiveFailures)
                    .build());
        } catch (Exception e) {
            log.warn("[Failover] Failed to record transition event: {}", e.getMessage());
        }
    }

    /** Get the current active DC identifier. Used by step executors to tag attempts. */
    public String getCurrentDc() {
        return activeDc;
    }

    public void close() {
        probe.close();
    }
}
