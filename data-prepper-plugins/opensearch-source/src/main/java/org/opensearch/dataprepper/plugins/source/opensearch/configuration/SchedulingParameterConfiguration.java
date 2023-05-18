/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.time.Instant;

public class SchedulingParameterConfiguration {

    @JsonProperty("rate")
    private Duration rate;

    @Min(1)
    @JsonProperty("job_count")
    private int jobCount = 1;

    @JsonProperty("start_time")
    private Instant startTime = Instant.now();

    public Duration getRate() {
        return rate;
    }

    public int getJobCount() {
        return jobCount;
    }

    public Instant getStartTime() {
        return startTime;
    }
}