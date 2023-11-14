/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

// TODO: Convert the queryMap sort value from SearchConfiguration to this class to be passed to SearchWithPit or SearchWithScroll
public class SortingOptions {

    private String fieldName;

    @JsonProperty("order")
    private String order = "asc";

    @JsonProperty("format")
    private String format;

    @JsonProperty("mode")
    private String mode;

    public String getFieldName() {
        return fieldName;
    }

    public String getOrder() {
        return order;
    }

    public String getFormat() {
        return format;
    }

    public String getMode() {
        return mode;
    }
}
