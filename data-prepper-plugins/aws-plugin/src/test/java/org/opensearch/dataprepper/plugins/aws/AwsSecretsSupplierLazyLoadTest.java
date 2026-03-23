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
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueResponse;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for lazy-loading behavior when skip_validation_on_start is true.
 */
@ExtendWith(MockitoExtension.class)
class AwsSecretsSupplierLazyLoadTest {

    private ObjectMapper objectMapper;
    private String testSecretId;
    private String testKey;
    private String testValue;

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
    private PutSecretValueRequest putSecretValueRequest;

    @Mock
    private PutSecretValueResponse putSecretValueResponse;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        testSecretId = UUID.randomUUID().toString();
        testKey = UUID.randomUUID().toString();
        testValue = UUID.randomUUID().toString();
    }

    @Test
    void testSecretWithSkipValidationOnStartTrue_LoadsOnFirstAccess() throws JsonProcessingException {
        // Given: Secret configured with skip_validation_on_start=true
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(testSecretId, awsSecretManagerConfiguration)
        );
        when(awsSecretManagerConfiguration.isSkipValidationOnStart()).thenReturn(true); // Skip on start
        when(awsSecretManagerConfiguration.createSecretManagerClient(awsCredentialsSupplier)).thenReturn(secretsManagerClient);
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(objectMapper.writeValueAsString(
                Map.of(testKey, testValue)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);

        // When: AwsSecretsSupplier is constructed
        final AwsSecretsSupplier supplier = new AwsSecretsSupplier(
                secretValueDecoder, awsSecretPluginConfig, objectMapper, awsCredentialsSupplier
        );

        // Then: Secret is NOT retrieved at construction time
        verify(secretsManagerClient, never()).getSecretValue(eq(getSecretValueRequest));

        // When: Secret is accessed for the first time
        final Object value = supplier.retrieveValue(testSecretId, testKey);

        // Then: Secret is loaded on-demand
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));
        assertThat(value, equalTo(testValue));
    }

    @Test
    void testSecretWithSkipValidationOnStartFalse_LoadsAtConstruction() throws JsonProcessingException {
        // Given: Secret configured with skip_validation_on_start=false (default)
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(testSecretId, awsSecretManagerConfiguration)
        );
        when(awsSecretManagerConfiguration.isSkipValidationOnStart()).thenReturn(false); // Load on start
        when(awsSecretManagerConfiguration.createSecretManagerClient(awsCredentialsSupplier)).thenReturn(secretsManagerClient);
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(objectMapper.writeValueAsString(
                Map.of(testKey, testValue)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);

        // When: AwsSecretsSupplier is constructed
        final AwsSecretsSupplier supplier = new AwsSecretsSupplier(
                secretValueDecoder, awsSecretPluginConfig, objectMapper, awsCredentialsSupplier
        );

        // Then: Secret IS retrieved at construction time
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));

        // When: Secret is accessed
        final Object value = supplier.retrieveValue(testSecretId, testKey);

        // Then: No additional retrieval (already loaded)
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));
        assertThat(value, equalTo(testValue));
    }

    @Test
    void testUpdateValue_withSkipValidationOnStart_loadsSecretBeforeUpdate() throws JsonProcessingException {
        // Given: Secret configured with skip_validation_on_start=true
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(testSecretId, awsSecretManagerConfiguration)
        );
        when(awsSecretManagerConfiguration.isSkipValidationOnStart()).thenReturn(true);
        when(awsSecretManagerConfiguration.createSecretManagerClient(awsCredentialsSupplier)).thenReturn(secretsManagerClient);
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(objectMapper.writeValueAsString(
                Map.of(testKey, testValue)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);
        when(awsSecretManagerConfiguration.putSecretValueRequest(any())).thenReturn(putSecretValueRequest);
        when(secretsManagerClient.putSecretValue(eq(putSecretValueRequest))).thenReturn(putSecretValueResponse);
        final String newVersionId = UUID.randomUUID().toString();
        when(putSecretValueResponse.versionId()).thenReturn(newVersionId);

        final AwsSecretsSupplier supplier = new AwsSecretsSupplier(
                secretValueDecoder, awsSecretPluginConfig, objectMapper, awsCredentialsSupplier
        );

        // Then: Secret is NOT retrieved at construction time
        verify(secretsManagerClient, never()).getSecretValue(eq(getSecretValueRequest));

        // When: updateValue is called before any retrieveValue
        final String versionId = supplier.updateValue(testSecretId, testKey, "newValue");

        // Then: Secret was loaded on-demand and update succeeded
        verify(secretsManagerClient, times(1)).getSecretValue(eq(getSecretValueRequest));
        assertThat(versionId, equalTo(newVersionId));
    }
}
