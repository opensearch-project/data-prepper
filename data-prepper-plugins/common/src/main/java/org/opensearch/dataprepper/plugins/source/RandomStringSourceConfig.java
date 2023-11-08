/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;

public class RandomStringSourceConfig {
    @JsonProperty("wait_delay")
    private Duration waitDelay = Duration.ofMillis(500);

    public Duration getWaitDelay() {
        return waitDelay;
    }
}
