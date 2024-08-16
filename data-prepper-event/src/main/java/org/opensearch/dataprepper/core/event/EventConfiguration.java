/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data Prepper configurations for events.
 */
public class EventConfiguration {
    @JsonProperty("maximum_cached_keys")
    private Integer maximumCachedKeys = 512;

    public static EventConfiguration defaultConfiguration() {
        return new EventConfiguration();
    }

    /**
     * Gets the maximum number of cached {@link org.opensearch.dataprepper.model.event.EventKey} objects.
     *
     * @return the cache maximum count
     */
    Integer getMaximumCachedKeys() {
        return maximumCachedKeys;
    }
}
