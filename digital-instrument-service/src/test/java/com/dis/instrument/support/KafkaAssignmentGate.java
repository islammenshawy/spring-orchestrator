package com.dis.instrument.support;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * TEST-ONLY startup gate (lives on the test classpath, never packaged): blocks context
 * initialization until every Kafka listener container has partition assignments.
 *
 * Why: the test profile uses {@code auto-offset-reset: latest}. A test that publishes commands
 * the moment the context is "up" races the consumer group join — anything published before
 * assignment is silently skipped (the cold-start "stalled @ INIT" flake). With execution lanes the
 * per-context consumer count grew (lane concurrency + retry tiers), widening that window and
 * making the flake frequent. Waiting for assignment removes the race deterministically instead
 * of papering over it with sleeps in individual tests.
 * Runs on ApplicationReadyEvent — AFTER the Lifecycle phase (which starts @KafkaListener
 * containers) and after the library's SmartInitializingSingleton (which starts the programmatic
 * reply containers). Gating any earlier (e.g. SmartInitializingSingleton) deadlocks: singleton
 * callbacks run sequentially on the main thread, so a gate there blocks the very phase that
 * would start the containers it waits for.
 */
@Slf4j
@Component
public class KafkaAssignmentGate {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private final KafkaListenerEndpointRegistry registry;
    private final List<ConcurrentMessageListenerContainer<?, ?>> programmaticContainers;

    public KafkaAssignmentGate(KafkaListenerEndpointRegistry registry,
                               List<ConcurrentMessageListenerContainer<?, ?>> programmaticContainers) {
        this.registry = registry;
        this.programmaticContainers = programmaticContainers;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void awaitAssignments() {
        List<MessageListenerContainer> containers = new ArrayList<>(registry.getListenerContainers());
        containers.addAll(programmaticContainers);

        Instant deadline = Instant.now().plus(TIMEOUT);
        for (MessageListenerContainer container : containers) {
            while (!hasAssignment(container)) {
                if (Instant.now().isAfter(deadline)) {
                    log.warn("[TestGate] Timed out waiting for assignment on container {} — proceeding",
                            container.getListenerId());
                    break; // move on to the next container, don't abandon the rest
                }
                if (!container.isRunning()) {
                    // Containers are started by the library's SmartInitializingSingleton; if this
                    // one isn't running yet, give the startup sequence a moment.
                    sleep(100);
                    continue;
                }
                sleep(100);
            }
        }

        // Assignment being non-empty is NOT enough: a concurrency-N container's members join one
        // by one, and every join rebalances partitions onto fresh members. Wait until the overall
        // assignment is STABLE (unchanged for 3 consecutive seconds) so tests don't publish into
        // the rebalance churn window.
        int lastTotal = -1;
        int stableChecks = 0;
        while (stableChecks < 3 && Instant.now().isBefore(deadline)) {
            int total = containers.stream().mapToInt(this::assignedCount).sum();
            if (total == lastTotal && total > 0) {
                stableChecks++;
            } else {
                stableChecks = 0;
                lastTotal = total;
            }
            sleep(1000);
        }
        log.info("[TestGate] All {} Kafka containers assigned and stable ({} partitions) — context ready",
                containers.size(), lastTotal);
    }

    private boolean hasAssignment(MessageListenerContainer container) {
        return assignedCount(container) > 0;
    }

    private int assignedCount(MessageListenerContainer container) {
        var assigned = container.getAssignedPartitions();
        return assigned == null ? 0 : assigned.size();
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
