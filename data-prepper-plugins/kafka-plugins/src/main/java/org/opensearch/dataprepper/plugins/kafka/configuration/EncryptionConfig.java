package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EncryptionConfig {
    @JsonProperty("type")
    private EncryptionType type = EncryptionType.SSL;

    @JsonProperty("insecure")
    private boolean insecure = false;

    public EncryptionType getType() {
        return type;
    }

    public boolean getInsecure() {
        return insecure;
    }
}
