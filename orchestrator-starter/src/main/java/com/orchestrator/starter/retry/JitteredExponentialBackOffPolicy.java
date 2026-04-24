package com.orchestrator.starter.retry;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.ThreadWaitSleeper;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with configurable jitter to prevent thundering herd.
 * Implements SleepingBackOffPolicy for Spring Kafka RetryTopicConfigurationBuilder.
 *
 * jitterFactor=0.0: no jitter (2s → 4s → 8s — all retries aligned)
 * jitterFactor=0.5: equal jitter (1-2s → 2-4s → 4-8s — balanced spread)
 * jitterFactor=1.0: full jitter (0-2s → 0-4s → 0-8s — maximum spread)
 */
@Slf4j
@Setter
public class JitteredExponentialBackOffPolicy implements SleepingBackOffPolicy<JitteredExponentialBackOffPolicy> {

    private long initialInterval = 2000;
    private double multiplier = 2.0;
    private long maxInterval = 30000;
    private double jitterFactor = 0.5;
    private Sleeper sleeper = new ThreadWaitSleeper();

    @Override
    public JitteredExponentialBackOffPolicy withSleeper(Sleeper sleeper) {
        JitteredExponentialBackOffPolicy copy = new JitteredExponentialBackOffPolicy();
        copy.initialInterval = this.initialInterval;
        copy.multiplier = this.multiplier;
        copy.maxInterval = this.maxInterval;
        copy.jitterFactor = this.jitterFactor;
        copy.sleeper = sleeper;
        return copy;
    }

    @Override
    public BackOffContext start(RetryContext context) {
        return new JitteredContext(initialInterval);
    }

    @Override
    public void backOff(BackOffContext ctx) throws BackOffInterruptedException {
        JitteredContext jctx = (JitteredContext) ctx;
        long base = jctx.nextDelay;
        long jitterRange = (long) (base * jitterFactor);
        long fixed = base - jitterRange;
        long jitter = jitterRange > 0 ? ThreadLocalRandom.current().nextLong(0, jitterRange + 1) : 0;
        long actual = fixed + jitter;

        log.debug("[Backoff] base={}ms jitter=+{}ms actual={}ms", base, jitter, actual);
        try {
            sleeper.sleep(actual);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackOffInterruptedException("Interrupted", e);
        }
        jctx.nextDelay = Math.min((long) (jctx.nextDelay * multiplier), maxInterval);
    }

    private static class JitteredContext implements BackOffContext {
        long nextDelay;
        JitteredContext(long initial) { this.nextDelay = initial; }
    }
}
