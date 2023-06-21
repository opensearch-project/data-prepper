/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class ServiceTypeOptions {

    @JsonProperty("maxmind")
    @NotNull
    private MaxMindServiceConfig maxMindService;

    /**
     * Get the MaxMind Service Configuration
     * @return MaxMindServiceConfig
     */
    public MaxMindServiceConfig getMaxMindService() {
        return maxMindService;
    }
}