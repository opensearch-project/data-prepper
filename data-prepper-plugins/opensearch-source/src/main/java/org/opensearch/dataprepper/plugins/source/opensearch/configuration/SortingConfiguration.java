/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
public class SortingConfiguration {
        private static final String DEFAULT_SORT_ORDER = "asc";

        @NotEmpty
        @NotNull
        @JsonProperty("sort_key")
        private String sortKey;
        @JsonProperty("order")
        private String order = DEFAULT_SORT_ORDER;

        public String getSortKey() {
            return sortKey;
        }

        public String getOrder() {
            return order;
        }
}
