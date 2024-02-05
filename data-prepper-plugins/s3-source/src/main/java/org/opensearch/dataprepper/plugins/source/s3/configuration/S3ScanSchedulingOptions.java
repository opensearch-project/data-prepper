/*
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;

import java.time.Duration;

public class S3ScanSchedulingOptions {

    @JsonProperty("interval")
    @NotNull
    @DurationMin(seconds = 1L, message = "S3 scan interval must be at least 1 second")
    @DurationMax(days = 365L, message = "S3 scan interval must be less than or equal to 365 days")
    private Duration interval;

    @Min(2)
    @JsonProperty("count")
    private int count = Integer.MAX_VALUE;

    public Duration getInterval() {
        return interval;
    }

    public int getCount() {
        return count;
    }

}
