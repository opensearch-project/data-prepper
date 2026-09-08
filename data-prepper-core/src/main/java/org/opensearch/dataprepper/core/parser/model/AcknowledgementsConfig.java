/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;

/**
 * Holds configuration for acknowledgements within {@link DataPrepperConfiguration}.
 */
public class AcknowledgementsConfig {
    static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    public static AcknowledgementsConfig defaultConfiguration() {
        return new AcknowledgementsConfig();
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    @JsonProperty("shutdown_timeout")
    void setShutdownTimeout(final Duration shutdownTimeout) {
        if (shutdownTimeout == null || shutdownTimeout.isNegative() || shutdownTimeout.isZero()) {
            throw new IllegalArgumentException("acknowledgements shutdown_timeout must be positive.");
        }
        this.shutdownTimeout = shutdownTimeout;
    }
}
