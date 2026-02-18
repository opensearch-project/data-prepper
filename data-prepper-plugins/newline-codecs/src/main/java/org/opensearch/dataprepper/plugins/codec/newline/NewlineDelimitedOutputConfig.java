/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.codec.newline;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for the newline delimited output codec.
 */
public class NewlineDelimitedOutputConfig {

    /**
     * When false (default), events with no message or an empty message are not written.
     * When true, empty messages are written as blank lines, similar to ndjson input codec.
     */
    @JsonProperty("include_empty_objects")
    private boolean includeEmptyObjects = false;

    public boolean isIncludeEmptyObjects() {
        return includeEmptyObjects;
    }

    public void setIncludeEmptyObjects(final boolean includeEmptyObjects) {
        this.includeEmptyObjects = includeEmptyObjects;
    }
}