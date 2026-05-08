/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.annotations;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Data Prepper plugin or plugin configuration field as experimental.
 * <p>
 * Experimental features do not have the same compatibility guarantees as other features and may be unstable.
 * They may have breaking changes between minor versions and may even be removed.
 * <p>
 * When applied to a plugin class ({@link ElementType#TYPE}), the entire plugin is experimental.
 * When applied to a configuration field ({@link ElementType#FIELD}), only that specific
 * feature is experimental. In both cases, the Data Prepper administrator must enable
 * experimental features in order to use them.
 *
 * @since 2.11
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
@Constraint(validatedBy = {})
public @interface Experimental {
    String message() default "This feature is experimental. You must enable experimental features in data-prepper-config.yaml in order to use them.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
