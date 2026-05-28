package com.orchestrator.starter.failover;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Probes Kafka cluster health using AdminClient.describeCluster().
 * Maintains an AdminClient per DC for warm connections.
 */
@Slf4j
public class DcHealthProbe {

    private final Map<String, AdminClient> clients = new ConcurrentHashMap<>();
    private final Map<String, String> dcBootstraps;
    private final long timeoutMs;

    public DcHealthProbe(Map<String, String> dcBootstraps, long timeoutMs) {
        this.dcBootstraps = dcBootstraps;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Probe a DC's Kafka cluster. Returns true if healthy.
     * Uses describeCluster() which exercises the full Kafka protocol —
     * not just TCP connect (which can succeed against a wedged broker).
     */
    public boolean probe(String dcId) {
        try {
            AdminClient client = clients.computeIfAbsent(dcId, this::createClient);
            var result = client.describeCluster();
            var nodes = result.nodes().get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            var controller = result.controller().get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            boolean healthy = !nodes.isEmpty() && controller != null;
            if (!healthy) {
                log.warn("[DC-Probe] {} — {} nodes, controller={}", dcId, nodes.size(),
                        controller != null ? controller.id() : "NONE");
            }
            return healthy;
        } catch (Exception e) {
            log.warn("[DC-Probe] {} — failed: {}", dcId, e.getMessage());
            return false;
        }
    }

    private AdminClient createClient(String dcId) {
        String bootstrap = dcBootstraps.get(dcId);
        if (bootstrap == null) {
            throw new IllegalArgumentException("No bootstrap servers configured for DC: " + dcId);
        }
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "dc-health-probe-" + dcId);
        log.info("[DC-Probe] Created AdminClient for DC '{}' → {}", dcId, bootstrap);
        return AdminClient.create(props);
    }

    public void close() {
        clients.values().forEach(c -> {
            try { c.close(Duration.ofSeconds(5)); } catch (Exception ignored) {}
        });
        clients.clear();
    }
}
