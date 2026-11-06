/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * Marker interface for classes that provide pipeline transformation functions.
 * Classes implementing this interface can be referenced in rule YAML files via
 * the {@code function_providers} field and have their methods invoked dynamically
 * during pipeline template transformation.
 * <p>
 * Methods intended to be callable from templates must also be annotated with
 * {@link org.opensearch.dataprepper.model.annotations.TransformationFunction}.
 *
 * @since 2.12
 */
public interface PipelineTransformFunctionProvider {
}
