/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.DurationSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.time.LocalDateTime;

public class SchedulingParameterConfiguration {

    @JsonSerialize(using = DurationSerializer.class)
    @JsonDeserialize(using = DurationDeserializer.class)
    @JsonProperty("rate")
    private Duration rate;

    @Min(1)
    @JsonProperty("job_count")
    private int jobCount;

    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonProperty("start_time")
    private LocalDateTime startTime = LocalDateTime.now();

    public Duration getRate() {
        return rate;
    }

    public int getJobCount() {
        return jobCount;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }
}