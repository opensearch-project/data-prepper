package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.SecretManagerHelper;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class CredentialsConfig {
    // PlainText
    private final String username;
    private final String password;

    @JsonCreator
    public CredentialsConfig(@JsonProperty("type") final CredentialType type,
                             @JsonProperty("username") final String username,
                             @JsonProperty("password") final String password,
                             @JsonProperty("sts_role_arn") final String stsRoleArn,
                             @JsonProperty("region") final String region,
                             @JsonProperty("secretId") final String secretId) {
        switch (type) {
            case PLAINTEXT:
                if (username == null || password == null) {
                    throw new IllegalArgumentException("user and password must be set for plaintext credential type");
                }
                this.username = username;
                this.password = password;
                return;
            case SECRET_MANAGER:
                if (secretId == null || region == null) {
                    throw new IllegalArgumentException("secretId and region must be set for aws credential type");
                }
                final Map<String, String> secretMap = this.getSecretValueMap(stsRoleArn, region, secretId);
                if (!secretMap.containsKey("username") || !secretMap.containsKey("password")) {
                    throw new RuntimeException("username or password missing in secret manager.");
                }
                this.username = secretMap.get("username");
                this.password = secretMap.get("password");
                return;
            default:
                throw new IllegalArgumentException("unsupported credential type.");

        }
    }

    private Map<String, String> getSecretValueMap(String stsRoleArn, String region, String secretId) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            final String secretValue = SecretManagerHelper.getSecretValue(stsRoleArn, region, secretId);
            Map<String, String> secretValueMap = objectMapper.readValue(secretValue, Map.class);
            return secretValueMap;
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

    public enum CredentialType {
        PLAINTEXT("plaintext"),
        SECRET_MANAGER("secret_manager");
        private static final Map<String, CredentialType> OPTIONS_MAP = Arrays.stream(CredentialType.values())
                .collect(Collectors.toMap(
                        value -> value.type,
                        value -> value
                ));

        private final String type;

        CredentialType(final String type) {
            this.type = type;
        }

        @JsonCreator
        public static CredentialType fromTypeValue(final String type) {
            return OPTIONS_MAP.get(type.toLowerCase());
        }
    }
}
