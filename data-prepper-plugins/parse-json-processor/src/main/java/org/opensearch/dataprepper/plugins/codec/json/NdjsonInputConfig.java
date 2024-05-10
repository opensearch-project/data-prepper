/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the {@link NdjsonInputCodec} input codec.
 */
public class NdjsonInputConfig {
    /**
     * By default, we will not create events for empty objects. However, we will
     * permit users to include them if they desire.
     */
    @JsonProperty("include_empty_objects")
    private boolean includeEmptyObjects = false;

    public boolean isIncludeEmptyObjects() {
        return includeEmptyObjects;
    }
}
