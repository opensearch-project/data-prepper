/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;
import java.util.List;

public class KinesisSourceConfig {
    static final Duration DEFAULT_TIME_OUT_IN_MILLIS = Duration.ofMillis(1000);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;
    static final Duration DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT = Duration.ofMinutes(10);
    static final Duration DEFAULT_INITIALIZATION_BACKOFF_TIME = Duration.ofMillis(1000);
    static final int DEFAULT_MAX_INITIALIZATION_ATTEMPTS = Integer.MAX_VALUE;

    @Getter
    @JsonProperty("streams")
    @NotNull
    @Valid
    @Size(min = 1, max = 4, message = "Provide 1-4 streams to read from.")
    private List<KinesisStreamConfig> streams;

    @Getter
    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @Getter
    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_TIME_OUT_IN_MILLIS;

    @Getter
    @JsonProperty("records_to_accumulate")
    private int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

    @JsonProperty("acknowledgments")
    @Getter
    private boolean acknowledgments = false;

    @Getter
    @JsonProperty("consumer_strategy")
    private ConsumerStrategy consumerStrategy = ConsumerStrategy.ENHANCED_FAN_OUT;

    @Getter
    @JsonProperty("polling")
    private KinesisStreamPollingConfig pollingConfig;

    @Getter
    @NotNull
    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("shard_acknowledgment_timeout")
    private Duration shardAcknowledgmentTimeout = DEFAULT_SHARD_ACKNOWLEDGEMENT_TIMEOUT;

    public Duration getShardAcknowledgmentTimeout() {
        return shardAcknowledgmentTimeout;
    }

    @Getter
    @JsonProperty("max_initialization_attempts")
    private int maxInitializationAttempts = DEFAULT_MAX_INITIALIZATION_ATTEMPTS;

    @Getter
    @JsonProperty("initialization_backoff_time")
    private Duration initializationBackoffTime = DEFAULT_INITIALIZATION_BACKOFF_TIME;

    @Getter
    @JsonProperty("kcl_metrics_enabled")
    private boolean kclMetricsEnabled = true;
}



