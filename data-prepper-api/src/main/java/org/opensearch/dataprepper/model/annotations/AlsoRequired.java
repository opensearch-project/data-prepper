/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used in schema generation to define the names and corresponding values of other required
 * configurations if the configuration represented by the annotated field/method takes non-null or true value.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface AlsoRequired {
    /**
     * Array of Required annotations, each representing a required property with its allowed values.
     * @return returns array of required values
     */
    Required[] values();

    /**
     * Annotation to represent a required property and its allowed values.
     */
    @interface Required {
        /**
         * Name of the required property.
         * @return returns name
         */
        String name();

        /**
         * Allowed values for the required property. The default value of {} means any non-null value is allowed.
         * @return returns array of allowed values
         */
        String[] allowedValues() default {};
    }
}
