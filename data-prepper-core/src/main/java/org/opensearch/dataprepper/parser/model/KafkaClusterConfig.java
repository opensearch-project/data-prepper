package org.opensearch.dataprepper.parser.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import org.opensearch.dataprepper.model.plugin.kafka.AuthConfig;
import org.opensearch.dataprepper.model.plugin.kafka.AwsConfig;
import org.opensearch.dataprepper.model.plugin.kafka.EncryptionConfig;

import java.util.List;

public class KafkaClusterConfig {

    @JsonProperty("bootstrap_servers")
    private List<String> bootstrapServers;

    @Valid
    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("encryption")
    private EncryptionConfig encryptionConfig;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    public List<String> getBootStrapServers() {
        return bootstrapServers;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public EncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }
}
