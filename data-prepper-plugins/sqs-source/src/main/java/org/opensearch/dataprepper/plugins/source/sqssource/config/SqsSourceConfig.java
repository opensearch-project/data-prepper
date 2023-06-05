/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;

public class SqsSourceConfig {

    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);

    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("queues")
    private QueuesOptions queues;
    @JsonProperty("aws")
    private AwsAuthenticationOptions aws;

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_BUFFER_TIMEOUT;

    @JsonProperty("records_to_accumulate")
    private int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    public QueuesOptions getQueues() {
        return queues;
    }

    public AwsAuthenticationOptions getAws() {
        return aws;
    }

    public boolean getAcknowledgements() {
        return acknowledgments;
    }

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    public int getNumberOfRecordsToAccumulate() {
        return numberOfRecordsToAccumulate;
    }

    public PluginModel getCodec() {
        return codec;
    }
}
