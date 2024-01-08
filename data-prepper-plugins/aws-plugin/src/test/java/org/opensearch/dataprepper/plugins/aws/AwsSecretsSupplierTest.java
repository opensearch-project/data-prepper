/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.aws.AwsSecretsSupplier.MAP_TYPE_REFERENCE;

@ExtendWith(MockitoExtension.class)
class AwsSecretsSupplierTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_AWS_SECRET_CONFIGURATION_NAME = "test-secret-config";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "test-value";

    @Mock
    private AwsSecretManagerConfiguration awsSecretManagerConfiguration;

    @Mock
    private SecretValueDecoder secretValueDecoder;

    @Mock
    private AwsSecretPluginConfig awsSecretPluginConfig;

    @Mock
    private SecretsManagerClient secretsManagerClient;

    @Mock
    private GetSecretValueRequest getSecretValueRequest;

    @Mock
    private GetSecretValueResponse getSecretValueResponse;

    @Mock
    private SecretsManagerException secretsManagerException;

    private AwsSecretsSupplier objectUnderTest;

    @BeforeEach
    void setUp() throws JsonProcessingException {
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(TEST_AWS_SECRET_CONFIGURATION_NAME, awsSecretManagerConfiguration)
        );
        when(awsSecretManagerConfiguration.createSecretManagerClient()).thenReturn(secretsManagerClient);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(OBJECT_MAPPER.writeValueAsString(
                Map.of(TEST_KEY, TEST_VALUE)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);
        objectUnderTest = new AwsSecretsSupplier(secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER);
    }

    @Test
    void testRetrieveValueExists() {
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME, TEST_KEY), equalTo(TEST_VALUE));
    }

    @Test
    void testRetrieveValueMissingSecretConfigName() {
        assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.retrieveValue("missing-config-id", TEST_KEY));
    }

    @Test
    void testRetrieveValueMissingKey() {
        assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME, "missing-key"));
    }

    @Test
    void testRetrieveValueInvalidKeyValuePair() {
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(TEST_VALUE);
        objectUnderTest = new AwsSecretsSupplier(secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER);
        final Exception exception = assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME, TEST_KEY));
        assertThat(exception.getMessage(), equalTo(String.format("The value under secretId: %s is not a valid json.",
                TEST_AWS_SECRET_CONFIGURATION_NAME)));
    }

    @Test
    void testRetrieveValueBySecretIdOnlyDoesNotExist() {
        assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.retrieveValue("missing-config-id"));
    }

    @Test
    void testRetrieveValueBySecretIdOnlyNotSerializable() throws JsonProcessingException {
        final ObjectMapper mockedObjectMapper = mock(ObjectMapper.class);
        final JsonProcessingException mockedJsonProcessingException = mock(JsonProcessingException.class);
        final String testValue = "{\"a\":\"b\"}";
        when(mockedObjectMapper.readValue(eq(testValue), eq(MAP_TYPE_REFERENCE))).thenReturn(Map.of("a", "b"));
        when(mockedObjectMapper.writeValueAsString(ArgumentMatchers.any())).thenThrow(mockedJsonProcessingException);
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(testValue);
        objectUnderTest = new AwsSecretsSupplier(secretValueDecoder, awsSecretPluginConfig, mockedObjectMapper);
        final Exception exception = assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME));
        assertThat(exception.getMessage(), equalTo(String.format("Unable to read the value under secretId: %s as string.",
                TEST_AWS_SECRET_CONFIGURATION_NAME)));
    }

    @ParameterizedTest
    @ValueSource(strings = {TEST_VALUE, "{\"a\":\"b\"}"})
    void testRetrieveValueWithoutKey(String testValue) {
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(testValue);
        objectUnderTest = new AwsSecretsSupplier(secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER);
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME), equalTo(testValue));
    }

    @Test
    void testConstructorWithGetSecretValueFailure() {
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenThrow(secretsManagerException);
        assertThrows(RuntimeException.class, () -> new AwsSecretsSupplier(
                secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER));
    }

    @Test
    void testRefreshSecretsWithKey() {
        final String testValue = "{\"key\":\"oldValue\"}";
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(testValue);
        objectUnderTest = new AwsSecretsSupplier(secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER);
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME, "key"),
                equalTo("oldValue"));
        final String newTestValue = "{\"key\":\"newValue\"}";
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(newTestValue);
        objectUnderTest.refresh(TEST_AWS_SECRET_CONFIGURATION_NAME);
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME, "key"),
                equalTo("newValue"));
    }

    @Test
    void testRefreshSecretsWithoutKey() {
        final String testValue = UUID.randomUUID().toString();
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(testValue);
        objectUnderTest = new AwsSecretsSupplier(secretValueDecoder, awsSecretPluginConfig, OBJECT_MAPPER);
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME), equalTo(testValue));
        final String newTestValue = testValue + "-mutated";
        when(secretValueDecoder.decode(eq(getSecretValueResponse))).thenReturn(newTestValue);
        objectUnderTest.refresh(TEST_AWS_SECRET_CONFIGURATION_NAME);
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME), equalTo(newTestValue));
    }
}