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

public class SemanticFieldMapping {

    @JsonProperty("name")
    private String name;

    @JsonProperty("language")
    private SemanticEnrichmentLanguage language;

    public String getName() {
        return name;
    }

    public SemanticEnrichmentLanguage getLanguage() {
        return language;
    }
}
