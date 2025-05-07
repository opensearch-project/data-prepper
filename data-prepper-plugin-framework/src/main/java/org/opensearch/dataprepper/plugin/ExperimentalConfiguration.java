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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Data Prepper configurations for experimental features.
 *
 * @since 2.11
 */
public class ExperimentalConfiguration {
    @JsonProperty("enable_all")
    private boolean enableAll = false;

    @JsonProperty("enabled_plugins")
    private Map<String, Set<String>> enabledPlugins;

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

    /**
     * Gets enabled plugins by plugin type.
     *
     * @return A map of plugin types to list of allowed plugins by name.
     * @since 2.12
     */
    public Map<String, Set<String>> getEnabledPlugins() {
        return enabledPlugins != null ? enabledPlugins : Collections.emptyMap();
    }
}
