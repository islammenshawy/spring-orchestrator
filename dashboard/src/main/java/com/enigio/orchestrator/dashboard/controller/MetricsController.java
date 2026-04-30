package com.enigio.orchestrator.dashboard.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private record AppInfo(String name, String pattern, String baseUrl, String color) {}

    private static final List<AppInfo> APPS = List.of(
            new AppInfo("Starter Library", "library", "http://localhost:8085", "#22c55e"),
            new AppInfo("Digital Instrument", "dis", "http://localhost:8087", "#f97316"),
            new AppInfo("Saga+Outbox", "saga", "http://localhost:8082", "#a78bfa"),
            new AppInfo("Statemachine", "statemachine", "http://localhost:8083", "#22d3ee"),
            new AppInfo("Spring Integration", "spring-integration", "http://localhost:8084", "#818cf8")
    );

    @GetMapping("/compare")
    public ResponseEntity<List<Map<String, Object>>> compareMetrics() {
        List<Map<String, Object>> results = new ArrayList<>();

        for (AppInfo app : APPS) {
            Map<String, Object> metrics = new LinkedHashMap<>();
            metrics.put("name", app.name);
            metrics.put("pattern", app.pattern);
            metrics.put("color", app.color);

            WebClient client = WebClient.create(app.baseUrl);
            try {
                // JVM threads
                metrics.put("threadCount", fetchMetricValue(client, "jvm.threads.live"));
                metrics.put("threadPeak", fetchMetricValue(client, "jvm.threads.peak"));
                metrics.put("threadDaemon", fetchMetricValue(client, "jvm.threads.daemon"));

                // JVM memory
                metrics.put("heapUsedMB", fetchMetricValue(client, "jvm.memory.used", "area", "heap") / (1024.0 * 1024));
                metrics.put("heapMaxMB", fetchMetricValue(client, "jvm.memory.max", "area", "heap") / (1024.0 * 1024));
                metrics.put("nonHeapUsedMB", fetchMetricValue(client, "jvm.memory.used", "area", "nonheap") / (1024.0 * 1024));

                // Process CPU
                metrics.put("cpuUsage", fetchMetricValue(client, "process.cpu.usage"));
                metrics.put("systemCpuUsage", fetchMetricValue(client, "system.cpu.usage"));

                // GC
                metrics.put("gcPauseCount", fetchMetricValue(client, "jvm.gc.pause", null, null, "count"));
                metrics.put("gcPauseTotalMs", fetchMetricValue(client, "jvm.gc.pause", null, null, "total_time") * 1000);

                // Tomcat threads
                metrics.put("tomcatThreadsBusy", fetchMetricValue(client, "tomcat.threads.busy"));
                metrics.put("tomcatThreadsMax", fetchMetricValue(client, "tomcat.threads.config.max"));

                metrics.put("status", "UP");
            } catch (Exception e) {
                metrics.put("status", "DOWN");
                metrics.put("error", e.getMessage());
            }
            results.add(metrics);
        }

        return ResponseEntity.ok(results);
    }

    private double fetchMetricValue(WebClient client, String metricName) {
        return fetchMetricValue(client, metricName, null, null, "value");
    }

    private double fetchMetricValue(WebClient client, String metricName, String tagKey, String tagValue) {
        return fetchMetricValue(client, metricName, tagKey, tagValue, "value");
    }

    @SuppressWarnings("unchecked")
    private double fetchMetricValue(WebClient client, String metricName,
                                    String tagKey, String tagValue, String statistic) {
        try {
            String uri = "/actuator/metrics/" + metricName;
            if (tagKey != null && tagValue != null) {
                uri += "?tag=" + tagKey + ":" + tagValue;
            }

            Map<String, Object> response = client.get()
                    .uri(uri)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();

            if (response == null) return 0;

            List<Map<String, Object>> measurements = (List<Map<String, Object>>) response.get("measurements");
            if (measurements == null) return 0;

            for (Map<String, Object> m : measurements) {
                String stat = (String) m.get("statistic");
                if (stat != null && stat.equalsIgnoreCase(statistic)) {
                    Object val = m.get("value");
                    if (val instanceof Number) return ((Number) val).doubleValue();
                }
            }
            // Fallback: return first measurement
            if (!measurements.isEmpty()) {
                Object val = measurements.get(0).get("value");
                if (val instanceof Number) return ((Number) val).doubleValue();
            }
        } catch (Exception e) {
            // Metric not available
        }
        return 0;
    }
}
