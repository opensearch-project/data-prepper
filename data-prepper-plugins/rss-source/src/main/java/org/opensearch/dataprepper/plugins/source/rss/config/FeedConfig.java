/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

import java.time.Duration;

public class FeedConfig {

    @JsonProperty("url")
    @NotBlank(message = "feed url cannot be null or empty")
    private String url;

    @JsonProperty("polling_frequency")
    private Duration pollingFrequency;

    @JsonProperty("authentication")
    @Valid
    private AuthenticationConfig authentication;

    public String getUrl() {
        return url;
    }

    public Duration getPollingFrequency() {
        return pollingFrequency;
    }

    public AuthenticationConfig getAuthentication() {
        return authentication;
    }
}
