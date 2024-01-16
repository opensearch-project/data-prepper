/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.SecretManagerHelper;

import java.util.Map;

public class CredentialsConfig {
    private final String username;
    private final String password;

    @JsonCreator
    public CredentialsConfig(@JsonProperty("plaintext") final PlainText plainText,
                             @JsonProperty("secret_manager") final SecretManager secretManager) {
        if (plainText != null && secretManager != null) {
            throw new IllegalArgumentException("plaintext and secret_manager cannot both be set");
        }
        if (plainText != null) {
            if (plainText.username == null || plainText.password == null) {
                throw new IllegalArgumentException("user and password must be set for plaintext credentials");
            }
            this.username = plainText.username;
            this.password = plainText.password;
        } else if (secretManager != null) {
            if (secretManager.secretId == null || secretManager.region == null) {
                throw new IllegalArgumentException("secretId and region must be set for aws credential type");
            }
            final Map<String, String> secretMap = this.getSecretValueMap(secretManager.stsRoleArn, secretManager.region, secretManager.secretId);
            if (!secretMap.containsKey("username") || !secretMap.containsKey("password")) {
                throw new RuntimeException("username or password missing in secret manager.");
            }
            this.username = secretMap.get("username");
            this.password = secretMap.get("password");
        } else {
            throw new IllegalArgumentException("plaintext or secret_manager must be set");
        }
    }

    private Map<String, String> getSecretValueMap(String stsRoleArn, String region, String secretId) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            final String secretValue = SecretManagerHelper.getSecretValue(stsRoleArn, region, secretId);
            return objectMapper.readValue(secretValue, new TypeReference<>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to get credentials.", e);
        }
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static class PlainText {
        private String username;
        private String password;

        @JsonCreator
        public PlainText(@JsonProperty("username") String username,
                         @JsonProperty("password") String password) {
            this.username = username;
            this.password = password;
        }
    }

    public static class SecretManager {
        private String region;
        private String secretId;
        private String stsRoleArn;

        @JsonCreator
        public SecretManager(@JsonProperty("sts_role_arn") String stsRoleArn,
                             @JsonProperty("region") String region,
                             @JsonProperty("secretId") String secretId) {
            this.stsRoleArn = stsRoleArn;
            this.region = region;
            this.secretId = secretId;
        }
    }
}
