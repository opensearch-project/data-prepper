/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.plugin.FailedToUpdatePluginConfigValueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class AwsSecretsSupplier implements SecretsSupplier {
    static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };
    private static final Logger LOG = LoggerFactory.getLogger(AwsSecretsSupplier.class);
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

    @Override
    public String updateValue(String secretId, Object newValue) {
        return updateValue(secretId, null, newValue);
    }

    @Override
    public String updateValue(String secretId, String keyToUpdate, Object newValue) {
        Object currentSecretStore = secretIdToValue.get(secretId);
        if (currentSecretStore instanceof Map) {
            if (keyToUpdate == null) {
                throw new IllegalArgumentException(
                        String.format("Key to update cannot be null for a key value based secret. secretId: %s", secretId));
            }
            final Map<String, Object> keyValuePairs = (Map<String, Object>) currentSecretStore;
            keyValuePairs.put(keyToUpdate, newValue);
        } else {
            //This store is not a key value pair store. It is just a value store.
            //If we are here, either KeyToUpdate passed is null or we simply ignore it and just put value in the store
            secretIdToValue.put(secretId, newValue);
        }
        // assuming all the secrets are string based (not binary)
        String secretKeyValueMapAsString = (String) retrieveValue(secretId);
        AwsSecretManagerConfiguration awsSecretManagerConfiguration = awsSecretManagerConfigurationMap.get(secretId);
        PutSecretValueRequest putSecretValueRequest =
                awsSecretManagerConfiguration.putSecretValueRequest(secretKeyValueMapAsString);
        SecretsManagerClient secretsManagerClient = secretsManagerClientMap.get(secretId);

        try {
            final PutSecretValueResponse putSecretValueResponse = secretsManagerClient.putSecretValue(putSecretValueRequest);
            LOG.info("Updated key: {} in the secret {}. New version of the store is {}",
                    keyToUpdate, secretId, putSecretValueResponse.versionId());
            return putSecretValueResponse.versionId();
        } catch (Exception e) {
            throw new FailedToUpdatePluginConfigValueException(
                    String.format("Failed to update the secret: %s to put a new value for the key: %s",
                            awsSecretManagerConfiguration.getAwsSecretId(), keyToUpdate), e);
        }
    }
}
