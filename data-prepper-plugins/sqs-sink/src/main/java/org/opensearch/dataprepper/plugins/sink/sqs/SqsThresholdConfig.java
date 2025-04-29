/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

public class SqsThresholdConfig {
    public static final int DEFAULT_MESSAGES_PER_EVENT = 25;
    public static final ByteCount DEFAULT_MAX_MESSAGE_SIZE = ByteCount.parse("256kb");
    public static final long DEFAULT_FLUSH_INTERVAL_TIME = 30;

    @JsonProperty("max_events_per_message")
    @Size(min = 1, max = 1000, message = "batch_size amount should be between 1 to 1000")
    private int maxEventsPerMessage = DEFAULT_MESSAGES_PER_EVENT;

    @JsonProperty("max_message_size")
    private ByteCount maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;

    @JsonProperty("flush_interval")
    @DurationMin(seconds = 60)
    @DurationMax(seconds = 3600)
    private Duration flushInterval = Duration.ofSeconds(DEFAULT_FLUSH_INTERVAL_TIME);

    public long getMaxMessageSizeBytes() {
        return maxMessageSize.getBytes();
    }

    public int getMaxEventsPerMessage() {
        return maxEventsPerMessage;
    }

    public long getFlushInterval() {
        return flushInterval.getSeconds();
    }

}

