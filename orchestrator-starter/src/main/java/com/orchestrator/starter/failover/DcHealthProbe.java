package com.orchestrator.starter.failover;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Probes Kafka broker health using TCP socket connect.
 * Fast (~1ms success, timeout on failure), no AdminClient overhead.
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
        String bootstrap = dcBootstraps.get(dcId);
        if (bootstrap == null) return false;

        // Parse host:port from bootstrap (take first broker)
        String[] parts = bootstrap.split(",")[0].split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        // TCP connect probe — fast, reliable, no AdminClient overhead
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), (int) timeoutMs);
            return true;
        } catch (Exception e) {
            log.warn("[DC-Probe] {} ({}:{}) — unreachable: {}", dcId, host, port, e.getMessage());
            return false;
        }
    }


    public void close() {
        // No cached clients to close — each probe creates and closes its own
    }
}
