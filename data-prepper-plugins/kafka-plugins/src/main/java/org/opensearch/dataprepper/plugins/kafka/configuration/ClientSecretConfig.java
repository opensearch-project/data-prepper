package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientSecretConfig {

    @JsonProperty("key")
    private String key;
    @JsonProperty("secret_name")
    private String secretName;

    public String getKey() {
        return key;
    }

    public String getSecretName() {
        return secretName;
    }
}
