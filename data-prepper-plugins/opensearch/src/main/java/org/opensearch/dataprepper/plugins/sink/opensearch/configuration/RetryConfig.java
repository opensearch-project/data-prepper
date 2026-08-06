/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.time.Duration;

public class RetryConfig {

    private static final Duration DEFAULT_INITIAL_DELAY = Duration.ofMillis(50);
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofMinutes(10);
    private static final double DEFAULT_JITTER = 0.0;
    private static final boolean DEFAULT_ADAPTIVE = false;
    private static final int DEFAULT_DECAY_THRESHOLD = 3;

    @Getter
    @JsonProperty("initial_delay")
    private Duration initialDelay = DEFAULT_INITIAL_DELAY;

    @Getter
    @JsonProperty("max_delay")
    private Duration maxDelay = DEFAULT_MAX_DELAY;

    @Getter
    @JsonProperty("jitter")
    private double jitter = DEFAULT_JITTER;

    @Getter
    @JsonProperty("adaptive")
    private boolean adaptive = DEFAULT_ADAPTIVE;

    @Getter
    @JsonProperty("decay_threshold")
    private int decayThreshold = DEFAULT_DECAY_THRESHOLD;
}
