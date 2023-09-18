/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchConfiguration {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(SearchConfiguration.class);

    @JsonProperty("search_context_type")
    private SearchContextType searchContextType;

    @JsonProperty("batch_size")
    private Integer batchSize = 1000;

    public SearchContextType getSearchContextType() {
        return searchContextType;
    }

    public Integer getBatchSize() {
        return batchSize;
    }
}
