/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.aws.AwsSecretsPluginConfigValueTranslator.AWS_SECRETS_PREFIX;

@ExtendWith(MockitoExtension.class)
class AwsSecretsPluginConfigValueTranslatorTest {
    @Mock
    private SecretsSupplier secretsSupplier;

    private AwsSecretsPluginConfigValueTranslator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new AwsSecretsPluginConfigValueTranslator(secretsSupplier);
    }

    @Test
    void testGetPrefix() {
        assertThat(objectUnderTest.getPrefix(), equalTo(AWS_SECRETS_PREFIX));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "invalid secret id with space:secret_key"
    })
    void testTranslateInputNoMatch(final String input) {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.translate(input));
    }

    @Test
    void testTranslateSecretIdWithKeyMatch() {
        final String testSecretName = "valid@secret-manager_name";
        final String testSecretKey = UUID.randomUUID().toString();
        final String testSecretValue = UUID.randomUUID().toString();
        final String input = String.format("%s:%s", testSecretName, testSecretKey);
        when(secretsSupplier.retrieveValue(eq(testSecretName), eq(testSecretKey))).thenReturn(testSecretValue);
        assertThat(objectUnderTest.translate(input), equalTo(testSecretValue));
    }

    @Test
    void testTranslateSecretIdWithoutKeyMatch() {
        final String testSecretName = "valid@secret-manager_name";
        final String testSecretValue = UUID.randomUUID().toString();
        when(secretsSupplier.retrieveValue(eq(testSecretName))).thenReturn(testSecretValue);
        assertThat(objectUnderTest.translate(testSecretName), equalTo(testSecretValue));
    }
}