package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CompressionConfig {
    @JsonProperty("type")
    private CompressionType type = CompressionType.ZSTD;

    public CompressionType getType() {
        return type;
    }

    public static CompressionConfig getCompressionConfig(CompressionType type) {
        CompressionConfig config = new CompressionConfig();
        config.type = type;
        return config;
    }
}
