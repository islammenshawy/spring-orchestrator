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
            // Run probe on a virtual thread with hard timeout.
            // AdminClient.create() can hang on dead brokers despite socket timeouts.
            return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try (AdminClient client = createClient(dcId)) {
                    var result = client.describeCluster();
                    var nodes = result.nodes().get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    var controller = result.controller().get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    return !nodes.isEmpty() && controller != null;
                } catch (Exception e) {
                    return false;
                }
            }).get(timeoutMs * 2, java.util.concurrent.TimeUnit.MILLISECONDS);
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
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 100);
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "dc-health-probe-" + dcId);
        log.info("[DC-Probe] Created AdminClient for DC '{}' → {}", dcId, bootstrap);
        return AdminClient.create(props);
    }

    public void close() {
        // No cached clients to close — each probe creates and closes its own
    }
}
