/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public class FeedSourceConfig {

    public static final Duration DEFAULT_POLLING_FREQUENCY = Duration.ofMinutes(5);

    @JsonProperty("feeds")
    @NotEmpty(message = "at least one feed is required")
    @Valid
    private Map<String, FeedConfig> feeds;

    @JsonProperty("polling_frequency")
    private Duration pollingFrequency = DEFAULT_POLLING_FREQUENCY;

    @JsonProperty("workers")
    @Min(1)
    @Max(1000)
    private int workers = 1;

    public Map<String, FeedConfig> getFeeds() {
        return feeds;
    }

    public Duration getPollingFrequency() {
        return pollingFrequency;
    }

    public int getWorkers() {
        return workers;
    }

    public Duration resolvePollingFrequency(final FeedConfig feed) {
        return Objects.requireNonNullElse(feed.getPollingFrequency(), pollingFrequency);
    }
}
