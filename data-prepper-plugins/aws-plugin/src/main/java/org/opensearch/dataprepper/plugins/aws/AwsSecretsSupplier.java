package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.util.Map;
import java.util.stream.Collectors;

public class AwsSecretsSupplier implements SecretsSupplier {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    private final Map<String, Object> secretIdToValue;

    public AwsSecretsSupplier(final AwsSecretPluginConfig awsSecretPluginConfig) {
        secretIdToValue = toSecretMap(awsSecretPluginConfig);
    }

    private Map<String, Object> toSecretMap(final AwsSecretPluginConfig awsSecretPluginConfig) {
        final Map<String, SecretsManagerClient> secretsManagerClientMap = toSecretsManagerClientMap(
                awsSecretPluginConfig);
        final Map<String, AwsSecretManagerConfiguration> awsSecretManagerConfigurationMap = awsSecretPluginConfig
                .getAwsSecretManagerConfigurationMap();
        return secretsManagerClientMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    final String secretConfigurationId = entry.getKey();
                    final AwsSecretManagerConfiguration awsSecretManagerConfiguration =
                            awsSecretManagerConfigurationMap.get(secretConfigurationId);
                    final SecretsManagerClient secretsManagerClient = entry.getValue();
                    final GetSecretValueRequest getSecretValueRequest = awsSecretManagerConfiguration
                            .createGetSecretValueRequest();
                    final GetSecretValueResponse getSecretValueResponse;
                    try {
                        getSecretValueResponse = secretsManagerClient.getSecretValue(getSecretValueRequest);
                    } catch (Exception e) {
                        throw ResourceNotFoundException.builder()
                                .message(String.format("Unable to retrieve secret: %s",
                                        awsSecretManagerConfiguration.getAwsSecretId()))
                                .cause(e)
                                .build();
                    }

                    try {
                        return OBJECT_MAPPER.readValue(getSecretValueResponse.secretString(), MAP_TYPE_REFERENCE);
                    } catch (JsonProcessingException e) {
                        return getSecretValueResponse.secretString();
                    }
                }));
    }

    private Map<String, SecretsManagerClient> toSecretsManagerClientMap(
            final AwsSecretPluginConfig awsSecretPluginConfig) {
        return awsSecretPluginConfig.getAwsSecretManagerConfigurationMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    final AwsSecretManagerConfiguration awsSecretManagerConfiguration = entry.getValue();
                    return awsSecretManagerConfiguration.createSecretManagerClient();
                }));
    }

    @Override
    public String retrieveValue(String secretId, String key) {
        if (!secretIdToValue.containsKey(secretId)) {
            throw new IllegalArgumentException(String.format("Unable to find secretId: %s", secretId));
        }
        final Object keyValuePairs = secretIdToValue.get(secretId);
        if (!(keyValuePairs instanceof Map)) {
            throw new IllegalArgumentException(String.format("The value under secretId: %s is not a valid json.",
                    secretId));
        }
        final Map<String, String> keyValueMap = (Map<String, String>) keyValuePairs;
        if (!keyValueMap.containsKey(key)) {
            throw new IllegalArgumentException(String.format("Unable to find the value of key: %s under secretId: %s",
                    key, secretId));
        }
        return keyValueMap.get(key);
    }

    @Override
    public String retrieveValue(String secretId) {
        if (!secretIdToValue.containsKey(secretId)) {
            throw new IllegalArgumentException(String.format("Unable to find secretId: %s", secretId));
        }
        try {
            final Object secretValue = secretIdToValue.get(secretId);
            return secretValue instanceof Map ? OBJECT_MAPPER.writeValueAsString(secretIdToValue.get(secretId)) :
                    (String) secretValue;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Unable to read the value under secretId: %s as string",
                    secretId));
        }
    }
}
