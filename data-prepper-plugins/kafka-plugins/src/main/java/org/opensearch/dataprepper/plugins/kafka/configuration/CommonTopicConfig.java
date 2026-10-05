/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

/**
 * This class has topic configurations which are common to all Kafka plugins - source, buffer, and sink.
 * <p>
 * Be sure to only add to this configuration if the setting is applicable for all three types.
 */
public abstract class CommonTopicConfig implements TopicConfig {
    static final Duration DEFAULT_RETRY_BACKOFF = Duration.ofSeconds(10);
    static final Duration DEFAULT_RECONNECT_BACKOFF = Duration.ofSeconds(10);
    static final Duration DEFAULT_METADATA_MAX_AGE = Duration.ofMillis(300000);

    @JsonProperty("name")
    @NotNull
    @Valid
    private String name;

    @JsonProperty("retry_backoff")
    private Duration retryBackoff = DEFAULT_RETRY_BACKOFF;

    @JsonProperty("reconnect_backoff")
    private Duration reconnectBackoff = DEFAULT_RECONNECT_BACKOFF;

    @JsonProperty("metadata_max_age")
    private Duration metadataMaxAge = DEFAULT_METADATA_MAX_AGE;

    @Override
    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    @Override
    public Duration getReconnectBackoff() {
        return reconnectBackoff;
    }

    @Override
    public Duration getMetadataMaxAge() {
        return metadataMaxAge;
    }

    @Override
    public String getName() {
        return name;
    }
}
