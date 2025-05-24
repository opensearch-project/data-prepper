/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

/**
 * Configuration class for threshold settings.
 * This class will be automatically wired by Data-Prepper.
 */
@NoArgsConstructor
@Getter
class ThresholdConfig {

    /**
     * Max number of spans per batch.
     * Use 0 to disable event-count based flushing (unbounded).
     */
    @JsonProperty("max_events")
    @Min(value = 0, message = "max_events must be 0 (unbounded) or greater")
    private int maxEvents = 512;

    @JsonProperty("max_batch_size")
    private ByteCount maxBatchSize = ByteCount.parse("1mb");

    @JsonProperty("flush_timeout")
    @DurationMin(millis = 1, message = "flush_timeout must be at least 1ms")
    private Duration flushTimeout = Duration.ofMillis(200);
}
