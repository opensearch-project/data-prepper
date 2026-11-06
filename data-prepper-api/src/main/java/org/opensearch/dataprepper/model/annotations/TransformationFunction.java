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
 * Marks a method as a pipeline transformation function that can be invoked
 * dynamically from template YAML files via the {@code FUNCTION_NAME} placeholder.
 * <p>
 * Annotated methods must be {@code public static} and accept a single {@code String}
 * parameter, returning a {@code String} result.
 * <p>
 * The enclosing class must implement
 * {@link org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider}.
 *
 * @since 2.12
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface TransformationFunction {
}
