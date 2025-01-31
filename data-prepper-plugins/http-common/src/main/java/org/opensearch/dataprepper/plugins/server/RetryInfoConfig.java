/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.server;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RetryInfoConfig {
    final Duration DEFAULT_MIN_DELAY = Duration.ofMillis(100);
    final Duration DEFAULT_MAX_DELAY = Duration.ofMillis(2000);

    @JsonProperty(value = "min_delay", defaultValue = "100ms")
    private Duration minDelay;

    @JsonProperty(value = "max_delay", defaultValue = "2s")
    private Duration maxDelay;

    public RetryInfoConfig() {
        this.minDelay = DEFAULT_MIN_DELAY;
        this.maxDelay = DEFAULT_MAX_DELAY;
    }

    public RetryInfoConfig(Duration minDelay, Duration maxDelay) {
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
    }

    public Duration getMinDelay() {
        return minDelay;
    }

    public void setMinDelay(Duration minDelay) {
        this.minDelay = minDelay;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(Duration maxDelay) {
        this.maxDelay = maxDelay;
    }
}
