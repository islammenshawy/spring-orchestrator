package com.enigio.orchestrator.common.config;

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
 * Exponential backoff with configurable jitter.
 * Implements SleepingBackOffPolicy so it works with Spring Kafka's
 * RetryTopicConfigurationBuilder.customBackoff().
 *
 * Without jitter (jitterFactor=0):
 *   2s → 4s → 8s → 16s
 *   All retries hit at exact same intervals = thundering herd.
 *   If 100 flows fail at the same time, all 100 retry at t+2s, t+6s, t+14s.
 *
 * With full jitter (jitterFactor=1.0):
 *   random(0, 2s) → random(0, 4s) → random(0, 8s)
 *   Maximum spread — retries distributed evenly across the backoff window.
 *   Good for high contention scenarios.
 *
 * With equal jitter (jitterFactor=0.5, recommended):
 *   1s + random(0, 1s) → 2s + random(0, 2s) → 4s + random(0, 4s)
 *   Balanced — guarantees a minimum delay while still spreading retries.
 *   Prevents both thundering herd AND premature retries.
 *
 * Configurable via application.yml:
 *   kafka.retry.jitter-factor: 0.5
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
        return new JitteredBackOffContext(initialInterval);
    }

    @Override
    public void backOff(BackOffContext backOffContext) throws BackOffInterruptedException {
        JitteredBackOffContext ctx = (JitteredBackOffContext) backOffContext;
        long baseDelay = ctx.getNextDelay();

        // Equal jitter: delay = base * (1 - jitterFactor) + random(0, base * jitterFactor)
        long jitterRange = (long) (baseDelay * jitterFactor);
        long fixedPart = baseDelay - jitterRange;
        long jitter = jitterRange > 0 ? ThreadLocalRandom.current().nextLong(0, jitterRange + 1) : 0;
        long actualDelay = fixedPart + jitter;

        log.info("[Backoff] base={}ms jitter=+{}ms actual={}ms (factor={})",
                baseDelay, jitter, actualDelay, jitterFactor);

        try {
            sleeper.sleep(actualDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackOffInterruptedException("Thread interrupted during backoff", e);
        }

        ctx.advanceDelay(multiplier, maxInterval);
    }

    private static class JitteredBackOffContext implements BackOffContext {
        private long nextDelay;

        JitteredBackOffContext(long initialInterval) {
            this.nextDelay = initialInterval;
        }

        long getNextDelay() {
            return nextDelay;
        }

        void advanceDelay(double multiplier, long maxInterval) {
            nextDelay = Math.min((long) (nextDelay * multiplier), maxInterval);
        }
    }
}
