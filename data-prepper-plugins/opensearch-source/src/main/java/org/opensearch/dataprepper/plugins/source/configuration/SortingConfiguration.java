/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class SortingConfiguration {

    @JsonProperty("sort_key")
    private List<String> sortKey;

    @JsonProperty("order")
    private String order;

    public List<String> getSortKey() {
        return sortKey;
    }

    public String getOrder() {
        return order;
    }
}
