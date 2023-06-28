/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSinkConfig {

    @JsonProperty("bootstrap_servers")
    @NotNull
    @Size(min = 1, message = "Bootstrap servers can't be empty")
    private List<String> bootStrapServers;

    @JsonProperty("aws")
    @NotNull
    AwsDLQConfig dlqConfig;

    @JsonProperty("thread_wait_time")
    private Long threadWaitTime;

    @JsonProperty("compression_type")
    private String compressionType;

    @JsonProperty("batch_size")
    private Long batchSize;

    @JsonProperty("max_request_size")
    private String maxRequestSize;

    @JsonProperty("acks")
    private String acks;

    @JsonProperty("topics")
    private List<TopicConfig> topics;

    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("schema")
    @NotNull
    @Valid
    private SchemaConfig schemaConfig;

    @JsonProperty(value = "serde_format",defaultValue = "plaintext")
    private String serdeFormat;


    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public Long getBatchSize() {
        return batchSize;
    }

    public String getMaxRequestSize() {
        return maxRequestSize;
    }

    public String getAcks() {
        return acks;
    }


    public List<TopicConfig> getTopics() {
        return topics;
    }


    public AuthConfig getAuthConfig() {
        return authConfig;
    }


    public List<String> getBootStrapServers() {
        return bootStrapServers;
    }

    public AwsDLQConfig getDlqConfig() {
        return dlqConfig;
    }

    public String getSerdeFormat() {
        return serdeFormat;
    }

    public Long getThreadWaitTime() {
        return threadWaitTime;
    }


}
