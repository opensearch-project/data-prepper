/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Use this annotation to provide example values for plugin configuration.
 *
 * @since 2.11
 */
@Documented
@Retention(RUNTIME)
@Target({FIELD})
public @interface ExampleValues {
    /**
     * One or more examples.
     * @return the examples.
     * @since 2.11
     */
    Example[] value();

    /**
     * A single example.
     *
     * @since 2.11
     */
    @interface Example {
        /**
         * The example value
         * @return The example value
         *
         * @since 2.11
         */
        String value();

        /**
         * A description of the example value.
         *
         * @since 2.11
         */
        String description() default "";
    }
}
