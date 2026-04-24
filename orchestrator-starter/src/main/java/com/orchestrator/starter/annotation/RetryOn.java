package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares which HTTP status codes or exception types should trigger a retry
 * via Kafka retry topics (exponential backoff with jitter).
 *
 * If the step throws an exception matching any of these, it's wrapped in
 * RetryableStepException and routed to retry-0 → retry-1 → retry-2 → DLT.
 *
 * Usage:
 * <pre>
 * @RetryOn(httpStatus = {500, 502, 503, 429})
 * @RetryOn(exceptions = {TimeoutException.class, ConnectException.class})
 * </pre>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RetryOn {

    /** HTTP status codes that trigger retry */
    int[] httpStatus() default {};

    /** Exception types that trigger retry */
    Class<? extends Throwable>[] exceptions() default {};
}
