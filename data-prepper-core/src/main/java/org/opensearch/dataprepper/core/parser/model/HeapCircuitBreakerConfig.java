/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

/**
 * Configuration for the heap circuit breaker.
 */
public class HeapCircuitBreakerConfig {
    public static final Duration DEFAULT_RESET = Duration.ofSeconds(1);
    private static final Duration DEFAULT_CHECK_INTERVAL = Duration.ofMillis(500);
    @NotNull
    @JsonProperty("usage")
    private ByteCount usage;

    @JsonProperty("reset")
    private Duration reset = DEFAULT_RESET;

    @JsonProperty("check_interval")
    private Duration checkInterval = DEFAULT_CHECK_INTERVAL;

    /**
     * Gets the usage as a {@link ByteCount}. If the current Java heap usage
     * exceeds this value then the circuit breaker will be open.
     *
     * @return Usage threshold
     * @since 2.1
     */
    public ByteCount getUsage() {
        return usage;
    }

    /**
     * Gets the reset timeout. After tripping the circuit breaker, no new
     * checks until after this time has passed.
     *
     * @return The duration
     * @since 2.1
     */
    public Duration getReset() {
        return reset;
    }

    /**
     * Gets the check interval. This is the time between checks of the heap size.
     *
     * @return The check interval as a duration
     * @since 2.1
     */
    public Duration getCheckInterval() {
        return checkInterval;
    }
}
