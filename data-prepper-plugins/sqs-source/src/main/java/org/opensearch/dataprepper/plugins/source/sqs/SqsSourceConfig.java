/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;

public class SqsSourceConfig {

    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_BUFFER_TIMEOUT;

    @JsonProperty("queues")
    @NotNull
    @Valid
    private List<QueueConfig> queues;

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public boolean getAcknowledgements() {
        return acknowledgments;
    }

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    public List<QueueConfig> getQueues() {
        return queues;
    }
}
