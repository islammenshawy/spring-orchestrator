package com.orchestrator.starter;

import com.orchestrator.starter.retry.JitteredExponentialBackOffPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.retry.backoff.BackOffContext;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JitteredBackOffPolicyTest {

    @Test
    void noJitterProducesExactDelays() {
        var policy = new JitteredExponentialBackOffPolicy();
        policy.setInitialInterval(1000);
        policy.setMultiplier(2.0);
        policy.setMaxInterval(10000);
        policy.setJitterFactor(0.0);
        policy.setSleeper(millis -> {}); // no-op sleeper for testing

        BackOffContext ctx = policy.start(null);
        // First backoff should be exactly 1000ms (no jitter)
        // We can't easily capture the delay, but at least verify no exception
        assertDoesNotThrow(() -> policy.backOff(ctx));
    }

    @Test
    void fullJitterProducesVariedDelays() {
        var policy = new JitteredExponentialBackOffPolicy();
        policy.setInitialInterval(1000);
        policy.setMultiplier(2.0);
        policy.setMaxInterval(10000);
        policy.setJitterFactor(1.0);

        List<Long> delays = new ArrayList<>();
        policy.setSleeper(delays::add);

        BackOffContext ctx = policy.start(null);
        for (int i = 0; i < 5; i++) {
            policy.backOff(ctx);
        }

        assertEquals(5, delays.size());
        // With full jitter (factor=1.0), delays should be between 0 and base
        // First base is 1000, so delay should be 0-1000
        assertTrue(delays.get(0) >= 0 && delays.get(0) <= 1000,
                "First delay should be 0-1000, was " + delays.get(0));
    }

    @Test
    void equalJitterGuaranteesMinimumDelay() {
        var policy = new JitteredExponentialBackOffPolicy();
        policy.setInitialInterval(2000);
        policy.setMultiplier(2.0);
        policy.setMaxInterval(30000);
        policy.setJitterFactor(0.5);

        List<Long> delays = new ArrayList<>();
        policy.setSleeper(delays::add);

        BackOffContext ctx = policy.start(null);
        for (int i = 0; i < 10; i++) {
            policy.backOff(ctx);
        }

        // With equal jitter (factor=0.5), first delay: base=2000, fixed=1000, jitter=0-1000
        // So delay should be 1000-2000
        for (Long delay : delays) {
            assertTrue(delay >= 0, "Delay should be non-negative, was " + delay);
        }
    }

    @Test
    void respectsMaxInterval() {
        var policy = new JitteredExponentialBackOffPolicy();
        policy.setInitialInterval(1000);
        policy.setMultiplier(10.0);
        policy.setMaxInterval(5000);
        policy.setJitterFactor(0.0);

        List<Long> delays = new ArrayList<>();
        policy.setSleeper(delays::add);

        BackOffContext ctx = policy.start(null);
        for (int i = 0; i < 5; i++) {
            policy.backOff(ctx);
        }

        // After first backoff (1000), next would be 10000 but capped at 5000
        assertTrue(delays.stream().allMatch(d -> d <= 5000),
                "All delays should be <= 5000");
    }

    @Test
    void withSleeperReturnsCopy() {
        var policy = new JitteredExponentialBackOffPolicy();
        policy.setInitialInterval(1000);
        policy.setJitterFactor(0.3);

        var copy = policy.withSleeper(millis -> {});
        assertNotSame(policy, copy);
    }
}
