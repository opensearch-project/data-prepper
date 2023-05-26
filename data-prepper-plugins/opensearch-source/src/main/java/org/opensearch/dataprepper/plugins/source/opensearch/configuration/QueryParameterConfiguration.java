/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class QueryParameterConfiguration {

    @JsonProperty("fields")
    private List<String> fields;

    @JsonProperty("query_string")
    private String queryString;

    public List<String> getFields() {
        return fields;
    }
}
