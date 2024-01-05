package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class AwsSecretsSupplier implements SecretsSupplier {
    private static final Logger LOG = LoggerFactory.getLogger(AwsSecretsSupplier.class);
    static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    private final SecretValueDecoder secretValueDecoder;
    private final ObjectMapper objectMapper;
    private final Map<String, AwsSecretManagerConfiguration> awsSecretManagerConfigurationMap;
    private final Map<String, SecretsManagerClient> secretsManagerClientMap;
    private final ConcurrentMap<String, Object> secretIdToValue;

    public AwsSecretsSupplier(
            final SecretValueDecoder secretValueDecoder,
            final AwsSecretPluginConfig awsSecretPluginConfig, final ObjectMapper objectMapper) {
        this.secretValueDecoder = secretValueDecoder;
        this.objectMapper = objectMapper;
        awsSecretManagerConfigurationMap = awsSecretPluginConfig
                .getAwsSecretManagerConfigurationMap();
        secretsManagerClientMap = toSecretsManagerClientMap(awsSecretPluginConfig);
        secretIdToValue = toSecretMap(awsSecretManagerConfigurationMap);
    }

    private ConcurrentMap<String, Object> toSecretMap(
            final Map<String, AwsSecretManagerConfiguration> awsSecretManagerConfigurationMap) {
        return secretsManagerClientMap.entrySet().stream()
                .collect(Collectors.toConcurrentMap(Map.Entry::getKey, entry -> {
                    final String secretConfigurationId = entry.getKey();
                    final AwsSecretManagerConfiguration awsSecretManagerConfiguration =
                            awsSecretManagerConfigurationMap.get(secretConfigurationId);
                    final SecretsManagerClient secretsManagerClient = entry.getValue();
                    return retrieveSecretsFromSecretManager(awsSecretManagerConfiguration, secretsManagerClient);
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
    public Object retrieveValue(String secretId, String key) {
        if (!secretIdToValue.containsKey(secretId)) {
            throw new IllegalArgumentException(String.format("Unable to find secretId: %s", secretId));
        }
        final Object keyValuePairs = secretIdToValue.get(secretId);
        if (!(keyValuePairs instanceof Map)) {
            throw new IllegalArgumentException(String.format("The value under secretId: %s is not a valid json.",
                    secretId));
        }
        final Map<String, Object> keyValueMap = (Map<String, Object>) keyValuePairs;
        if (!keyValueMap.containsKey(key)) {
            throw new IllegalArgumentException(String.format("Unable to find the value of key: %s under secretId: %s",
                    key, secretId));
        }
        return keyValueMap.get(key);
    }

    @Override
    public Object retrieveValue(String secretId) {
        if (!secretIdToValue.containsKey(secretId)) {
            throw new IllegalArgumentException(String.format("Unable to find secretId: %s", secretId));
        }
        try {
            final Object secretValue = secretIdToValue.get(secretId);
            return secretValue instanceof Map ? objectMapper.writeValueAsString(secretValue) :
                    secretValue;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Unable to read the value under secretId: %s as string.",
                    secretId));
        }
    }

    @Override
    public void refresh(String secretConfigId) {
        LOG.info("Retrieving latest secrets in aws:secrets:{}.", secretConfigId);
        secretIdToValue.compute(secretConfigId, (key, oldValue) -> {
            final AwsSecretManagerConfiguration awsSecretManagerConfiguration =
                    awsSecretManagerConfigurationMap.get(key);
            final SecretsManagerClient secretsManagerClient =
                    secretsManagerClientMap.get(key);
            return retrieveSecretsFromSecretManager(awsSecretManagerConfiguration, secretsManagerClient);
        });
        LOG.info("Finished retrieving latest secret in aws:secrets:{}.", secretConfigId);
    }

    private Object retrieveSecretsFromSecretManager(final AwsSecretManagerConfiguration awsSecretManagerConfiguration,
                                                    final SecretsManagerClient secretsManagerClient) {
        final GetSecretValueRequest getSecretValueRequest = awsSecretManagerConfiguration
                .createGetSecretValueRequest();
        final GetSecretValueResponse getSecretValueResponse;
        try {
            getSecretValueResponse = secretsManagerClient.getSecretValue(getSecretValueRequest);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Unable to retrieve secret: %s",
                            awsSecretManagerConfiguration.getAwsSecretId()), e);
        }

        try {
            return objectMapper.readValue(secretValueDecoder.decode(getSecretValueResponse), MAP_TYPE_REFERENCE);
        } catch (JsonProcessingException e) {
            return secretValueDecoder.decode(getSecretValueResponse);
        }
    }
}
