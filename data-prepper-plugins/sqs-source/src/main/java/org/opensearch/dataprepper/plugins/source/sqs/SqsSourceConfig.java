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

    /**
     * Controls whether end-to-end delivery acknowledgment is enabled.
     * <p>
     * When {@code true} (default), messages are only deleted from SQS after the entire
     * pipeline confirms successful delivery to the sink. This prevents data loss when
     * the downstream sink is unavailable, a processor fails, or the worker crashes.
     * <p>
     * When {@code false}, messages are deleted from SQS immediately after being written
     * to the pipeline buffer. <strong>WARNING: This risks permanent data loss.</strong>
     * If the pipeline fails after buffering but before sink delivery, the message cannot
     * be recovered.
     * <p>
     * When using acknowledgments, ensure the SQS queue's visibility timeout exceeds the
     * expected end-to-end processing time. If processing takes longer than the visibility
     * timeout, messages may be redelivered, resulting in duplicates. Configure a
     * Dead Letter Queue (DLQ) to handle messages that repeatedly fail processing.
     */
    @JsonProperty("acknowledgments")
    private boolean acknowledgments = true;

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
