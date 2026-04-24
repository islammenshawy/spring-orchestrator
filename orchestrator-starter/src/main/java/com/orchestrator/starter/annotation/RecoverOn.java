package com.orchestrator.starter.annotation;

import java.lang.annotation.*;

/**
 * Declares conditions where the step should auto-recover instead of failing.
 * Typically used for idempotent vendor APIs that return conflict/duplicate errors.
 *
 * Example: vendor returns HTTP 409 "document already created" — this means a
 * previous attempt succeeded but we crashed before recording it. The step should
 * treat this as success and advance.
 *
 * Repeatable — you can declare multiple recovery conditions:
 * <pre>
 * @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
 * @RecoverOn(httpStatus = 422, message = "already signed", action = RecoverAction.SKIP)
 * </pre>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(RecoverOn.List.class)
public @interface RecoverOn {

    /** HTTP status code that indicates a recoverable state */
    int httpStatus();

    /** Optional: only recover if the error message contains this substring */
    String message() default "";

    /** What to do on recovery */
    RecoverAction action() default RecoverAction.SKIP;

    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @interface List {
        RecoverOn[] value();
    }
}
