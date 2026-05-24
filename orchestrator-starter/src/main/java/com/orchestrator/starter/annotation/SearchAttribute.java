package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a flow entity field as a search attribute.
 * The library auto-creates a MongoDB index on this field at startup
 * and includes it in the search API.
 *
 * <pre>
 * public class OrderFlow extends AbstractFlow {
 *     @SearchAttribute
 *     private String customerId;
 *
 *     @SearchAttribute
 *     private String region;
 * }
 * </pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SearchAttribute {
}
