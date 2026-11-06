/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer;

import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;

/**
 * Test class that implements PipelineTransformFunctionProvider but has NO methods
 * annotated with @TransformationFunction. Used to verify annotation validation at startup.
 */
public class ProviderWithNoAnnotatedMethods implements PipelineTransformFunctionProvider {
    public static String someMethod(String input) {
        return input;
    }
}
