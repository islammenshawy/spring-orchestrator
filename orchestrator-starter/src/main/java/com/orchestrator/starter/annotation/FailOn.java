package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares which HTTP status codes or exceptions should immediately fail
 * the flow with no retries.
 *
 * Usage:
 * <pre>
 * @FailOn(httpStatus = {400, 403, 404})
 * @FailOn(exceptions = {IllegalArgumentException.class})
 * </pre>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface FailOn {

    /** HTTP status codes that fail immediately */
    int[] httpStatus() default {};

    /** Exception types that fail immediately */
    Class<? extends Throwable>[] exceptions() default {};
}
