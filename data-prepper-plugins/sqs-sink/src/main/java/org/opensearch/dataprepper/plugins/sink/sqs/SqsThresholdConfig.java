/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.constraints.ByteCountMax;
import org.opensearch.dataprepper.model.constraints.ByteCountMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

public class SqsThresholdConfig {
    public static final int DEFAULT_MESSAGES_PER_EVENT = 25;
    public static final ByteCount DEFAULT_MAX_MESSAGE_SIZE = ByteCount.parse("256kb");
    public static final long DEFAULT_FLUSH_INTERVAL_TIME = 30;

    @JsonProperty(value = "max_events_per_message", defaultValue="25")
    @Min(1)
    @Max(1000)
    private int maxEventsPerMessage = DEFAULT_MESSAGES_PER_EVENT;

    @JsonProperty("max_message_size")
    @ByteCountMin("1b")
    @ByteCountMax("1mb")
    private ByteCount maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;

    @JsonProperty("flush_interval")
    @DurationMin(seconds = 0)
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

