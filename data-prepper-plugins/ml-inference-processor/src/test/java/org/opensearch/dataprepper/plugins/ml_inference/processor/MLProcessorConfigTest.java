/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.BuiltInConnectors;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MLProcessorConfigTest {

    private MLProcessorConfig config;
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() throws Exception {
        config = new MLProcessorConfig();
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        setField(config, "awsAuthenticationOptions", awsAuthenticationOptions);
    }

    static Stream<String> builtInModelIds() {
        return BuiltInConnectors.listBuiltInModelIds().stream();
    }

    // --- isJobStsRoleArnProvidedForDirectModel ---

    @Test
    void isJobStsRoleArnProvidedForDirectModel_whenModelIdIsNonBuiltIn_returnsTrue() throws Exception {
        setField(config, "modelId", "5yhOo5UBbBLKq7E_H3rQ"); // UUID-style ml-commons model ID
        assertThat(config.isJobStsRoleArnProvidedForDirectModel(), is(true));
    }

    @ParameterizedTest
    @MethodSource("builtInModelIds")
    void isJobStsRoleArnProvidedForDirectModel_whenBuiltInModelAndRoleArnSet_returnsTrue(final String modelId) throws Exception {
        setField(config, "modelId", modelId);
        when(awsAuthenticationOptions.getJobStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/JobRole");

        assertThat(config.isJobStsRoleArnProvidedForDirectModel(), is(true));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void isJobStsRoleArnProvidedForDirectModel_whenBuiltInModelAndRoleArnBlankOrNull_returnsFalse(final String roleArn) throws Exception {
        setField(config, "modelId", "amazon.titan-embed-text-v2:0");
        when(awsAuthenticationOptions.getJobStsRoleArn()).thenReturn(roleArn);

        assertThat(config.isJobStsRoleArnProvidedForDirectModel(), is(false));
    }

    @Test
    void isJobStsRoleArnProvidedForDirectModel_whenModelIdIsNull_returnsTrue() throws Exception {
        setField(config, "modelId", null);
        assertThat(config.isJobStsRoleArnProvidedForDirectModel(), is(true));
    }

    private static void setField(final Object target, final String name, final Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
