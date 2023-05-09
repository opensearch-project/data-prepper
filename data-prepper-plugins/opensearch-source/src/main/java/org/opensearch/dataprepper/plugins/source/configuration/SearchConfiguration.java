/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchConfiguration {

    @JsonProperty("batch_size")
    private Integer batchSize;

    @JsonProperty("expand_wildcards")
    private String expandWildcards;

    @JsonProperty("sorting")
    private SortingConfiguration sorting;

    public Integer getBatchSize() {
        return batchSize;
    }

    public String getExpandWildcards() {
        return expandWildcards;
    }

    public SortingConfiguration getSorting() {
        return sorting;
    }
}
