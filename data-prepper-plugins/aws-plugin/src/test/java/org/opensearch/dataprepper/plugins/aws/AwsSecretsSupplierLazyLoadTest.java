/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for lazy-loading behavior when validate_at_bootstrap is false.
 */
@ExtendWith(MockitoExtension.class)
class AwsSecretsSupplierLazyLoadTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_SECRET_ID = "test-secret";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "test-value";

    @Mock
    private SecretValueDecoder secretValueDecoder;

    @Mock
    private AwsSecretPluginConfig awsSecretPluginConfig;

    @Mock
    private AwsSecretManagerConfiguration awsSecretManagerConfiguration;

    @Mock
    private SecretsManagerClient secretsManagerClient;

    @Mock
    private GetSecretValueRequest getSecretValueRequest;

    @Mock
    private GetSecretValueResponse getSecretValueResponse;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Test
    void testSecretWithValidateAtBootstrapFalse_LoadsOnFirstAccess() throws JsonProcessingException {
        // Given: Secret configured with validate_at_bootstrap=false
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(TEST_SECRET_ID, awsSecretManagerConfiguration)
        );
        when(awsSecretManagerConfiguration.isValidateAtBootstrap()).thenReturn(false); // Skip at bootstrap
        when(awsSecretManagerConfiguration.createSecretManagerClient(awsCredentialsSupplier)).thenReturn(secretsManagerClient);
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(OBJECT_MAPPER.writeValueAsString(
                Map.of(TEST_KEY, TEST_VALUE)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);

        // When: AwsSecretsSupplier is constructed
        final AwsSecretsSupplier supplier = new AwsSecretsSupplier(
                secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER, awsCredentialsSupplier
        );

        // Then: Secret is NOT retrieved at construction time
        verify(secretsManagerClient, never()).getSecretValue(eq(getSecretValueRequest));

        // When: Secret is accessed for the first time
        final Object value = supplier.retrieveValue(TEST_SECRET_ID, TEST_KEY);

        // Then: Secret is loaded on-demand
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));
        assertThat(value, equalTo(TEST_VALUE));
    }

    @Test
    void testSecretWithValidateAtBootstrapTrue_LoadsAtConstruction() throws JsonProcessingException {
        // Given: Secret configured with validate_at_bootstrap=true (default)
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(TEST_SECRET_ID, awsSecretManagerConfiguration)
        );
        when(awsSecretManagerConfiguration.isValidateAtBootstrap()).thenReturn(true); // Load at bootstrap
        when(awsSecretManagerConfiguration.createSecretManagerClient(awsCredentialsSupplier)).thenReturn(secretsManagerClient);
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(OBJECT_MAPPER.writeValueAsString(
                Map.of(TEST_KEY, TEST_VALUE)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);

        // When: AwsSecretsSupplier is constructed
        final AwsSecretsSupplier supplier = new AwsSecretsSupplier(
                secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER, awsCredentialsSupplier
        );

        // Then: Secret IS retrieved at construction time
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));

        // When: Secret is accessed
        final Object value = supplier.retrieveValue(TEST_SECRET_ID, TEST_KEY);

        // Then: No additional retrieval (already loaded)
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));
        assertThat(value, equalTo(TEST_VALUE));
    }
}
