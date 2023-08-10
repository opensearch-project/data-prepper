/*
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;

import java.time.Duration;

public class S3ScanSchedulingOptions {
    @JsonProperty("interval")
    private Duration interval = Duration.ofHours(8);

    @Min(1)
    @JsonProperty("count")
    private int count = 1;

    public Duration getInterval() {
        return interval;
    }

    public int getCount() {
        return count;
    }

}
