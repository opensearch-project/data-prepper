/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data Prepper configurations for experimental features.
 *
 * @since 2.11
 */
public class ExperimentalConfiguration {
    @JsonProperty("enable_all")
    private boolean enableAll = false;

    public static ExperimentalConfiguration defaultConfiguration() {
        return new ExperimentalConfiguration();
    }

    /**
     * Gets whether all experimental features are enabled.
     * @return true if all experimental features are enabled, false otherwise
     * @since 2.11
     */
    public boolean isEnableAll() {
        return enableAll;
    }
}
