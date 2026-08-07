/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer;

import org.opensearch.dataprepper.model.annotations.TransformationFunction;

/**
 * Test class that does NOT implement PipelineTransformFunctionProvider.
 * Has a static initializer to verify it doesn't run if interface check rejects it.
 */
public class NonProviderWithStaticInit {
    static {
        System.setProperty("test.static.init.ran", "true");
    }

    @TransformationFunction
    public static String getValue(String input) {
        return input;
    }
}
