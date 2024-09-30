package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to represent a required property and its allowed values.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Required {
    /**
     * Name of the required property.
     */
    String name();

    /**
     * Allowed values for the required property. The default value of {} means any non-null value is allowed.
     */
    String[] allowedValues() default {};
}
