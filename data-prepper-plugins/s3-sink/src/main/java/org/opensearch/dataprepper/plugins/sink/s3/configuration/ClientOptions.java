/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import org.hibernate.validator.constraints.time.DurationMin;

import java.time.Duration;

public class ClientOptions {
    private static final int DEFAULT_MAX_CONNECTIONS = 50;
    private static final Duration DEFAULT_ACQUIRE_TIMEOUT = Duration.ofSeconds(10);

    @JsonProperty("max_connections")
    @Min(1)
    private int maxConnections = DEFAULT_MAX_CONNECTIONS;

    @JsonProperty("acquire_timeout")
    @DurationMin(seconds = 1)
    private Duration acquireTimeout = DEFAULT_ACQUIRE_TIMEOUT;

    public int getMaxConnections() {
        return maxConnections;
    }

    public Duration getAcquireTimeout() {
        return acquireTimeout;
    }
}
