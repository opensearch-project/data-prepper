/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public class FeedSourceConfig {

    public static final Duration DEFAULT_POLLING_FREQUENCY = Duration.ofMinutes(5);
    static final Duration MINIMUM_POLLING_FREQUENCY = Duration.ofSeconds(1);

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

    @AssertTrue(message = "polling_frequency must be at least 1s (global default and every per-feed override)")
    boolean isPollingFrequencyAboveMinimum() {
        if (pollingFrequency != null && pollingFrequency.compareTo(MINIMUM_POLLING_FREQUENCY) < 0) {
            return false;
        }
        if (feeds == null) {
            return true;
        }
        return feeds.values().stream()
                .map(FeedConfig::getPollingFrequency)
                .filter(Objects::nonNull)
                .noneMatch(frequency -> frequency.compareTo(MINIMUM_POLLING_FREQUENCY) < 0);
    }
}
