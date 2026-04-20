/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SemanticEnrichmentConfig {
    static final String DEFAULT_LANGUAGE = "english";

    @JsonProperty("fields")
    private List<String> fields;

    @JsonProperty("language")
    private String language = DEFAULT_LANGUAGE;

    public List<String> getFields() {
        return fields;
    }

    public String getLanguage() {
        return language;
    }
}
