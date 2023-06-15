/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Duration;
import java.util.List;

public class QueuesOptions {

    @JsonProperty("urls")
    @NotNull
    private List<String> urls;

    @JsonProperty("polling_frequency")
    private Duration pollingFrequency = Duration.ZERO;

    @JsonProperty("batch_size")
    @Max(10)
    private Integer batchSize = 10;

    @JsonProperty("number_of_threads")
    @Positive(message = "number_of_threads should be unsigned value")
    private Integer numberOfThreads = 1;

    public List<String> getUrls() {
        return urls;
    }

    public Duration getPollingFrequency() {
        return pollingFrequency;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getNumberOfThreads() {
        return numberOfThreads;
    }
}
