package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class KmsConfig {
    @JsonProperty("key_id")
    private String keyId;

    @JsonProperty("encryption_context")
    private Map<String, String> encryptionContext;

    public String getKeyId() {
        return keyId;
    }

    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }
}
