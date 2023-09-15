package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsSecretsSupplierTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_AWS_SECRET_CONFIGURATION_NAME = "test-secret-config";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "test-value";

    @Mock
    private AwsSecretManagerConfiguration awsSecretManagerConfiguration;

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
        when(getSecretValueResponse.secretString()).thenReturn(OBJECT_MAPPER.writeValueAsString(
                Map.of(TEST_KEY, TEST_VALUE)
        ));
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);
        objectUnderTest = new AwsSecretsSupplier(awsSecretPluginConfig);
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
        when(getSecretValueResponse.secretString()).thenReturn(TEST_VALUE);
        objectUnderTest = new AwsSecretsSupplier(awsSecretPluginConfig);
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

    @ParameterizedTest
    @ValueSource(strings = {TEST_VALUE, "{\"a\":\"b\"}"})
    void testRetrieveValueWithoutKey(String testValue) {
        when(getSecretValueResponse.secretString()).thenReturn(testValue);
        objectUnderTest = new AwsSecretsSupplier(awsSecretPluginConfig);
        assertThat(objectUnderTest.retrieveValue(TEST_AWS_SECRET_CONFIGURATION_NAME), equalTo(testValue));
    }

    @Test
    void testConstructorWithGetSecretValueFailure() {
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenThrow(secretsManagerException);
        assertThrows(RuntimeException.class, () -> new AwsSecretsSupplier(awsSecretPluginConfig));
    }
}