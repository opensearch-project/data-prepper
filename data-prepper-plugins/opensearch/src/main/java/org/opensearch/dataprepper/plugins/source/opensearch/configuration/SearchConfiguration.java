/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;

import java.util.List;

public class SearchConfiguration {

    @JsonProperty("search_context_type")
    private String searchContextType;

    @JsonProperty("batch_size")
    private Integer batchSize = 1000;

    @JsonProperty("sort")
    @Valid
    private List<SortConfig> sort;

    @JsonIgnore
    private SearchContextType searchContextTypeValue;

    public SearchContextType getSearchContextType() {
        return searchContextTypeValue;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public List<SortConfig> getSort() {
        return sort;
    }

    @AssertTrue(message = "search_context_type must be one of [ 'scroll', 'point_in_time', 'none' ]")
    boolean isSearchContextTypeValid() {
        try {
            if (searchContextType != null) {
                searchContextTypeValue = SearchContextType.valueOf(searchContextType.toUpperCase());
            }

            return true;
        } catch (final IllegalArgumentException e) {
            return false;
        }
    }
}
