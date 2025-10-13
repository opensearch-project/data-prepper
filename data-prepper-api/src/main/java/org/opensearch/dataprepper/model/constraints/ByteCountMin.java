/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.constraints;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use this annotation to ensure that the byte count is not below
 * a specified maximum value.
 *
 * @since 2.13
 */
@Constraint(validatedBy = {ByteCountMinValidator.class})
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ByteCountMin {
    /**
     * The minimum value defined as a {@link org.opensearch.dataprepper.model.types.ByteCount}
     * string.
     *
     * @return The byte count string
     * @since 2.13
     */
    String value();

    String message() default "The provided byte count is below the minimum allowed value.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
