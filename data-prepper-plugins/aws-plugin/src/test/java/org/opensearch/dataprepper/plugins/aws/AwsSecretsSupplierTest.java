package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
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
    void testConstructorWithGetSecretValueFailure() {
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenThrow(secretsManagerException);
        assertThrows(ResourceNotFoundException.class, () -> new AwsSecretsSupplier(awsSecretPluginConfig));
    }

    @Test
    void testConstructorWithSecretStringParsingFailure() {
        when(getSecretValueResponse.secretString()).thenReturn("{");
        assertThrows(RuntimeException.class, () -> new AwsSecretsSupplier(awsSecretPluginConfig));
    }
}