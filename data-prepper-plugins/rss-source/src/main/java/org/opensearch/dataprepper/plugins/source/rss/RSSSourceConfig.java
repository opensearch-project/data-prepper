/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

import java.time.Duration;

public class RSSSourceConfig {

    static final Duration DEFAULT_POLLING_FREQUENCY = Duration.ofMinutes(5);

    @JsonProperty("url")
    @NotBlank(message = "RSS Feed URL cannot be null or empty")
    private String url;

    @JsonProperty("polling_frequency")
    private Duration pollingFrequency = DEFAULT_POLLING_FREQUENCY;

    public String getUrl() {
        return url;
    }

    public Duration getPollingFrequency() {
        return pollingFrequency;
    }
}
