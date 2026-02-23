/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableConfig {

    @JsonProperty("table_name")
    @NotBlank
    private String tableName;

    @JsonProperty("catalog")
    private Map<String, String> catalog = Collections.emptyMap();

    @JsonProperty("identifier_columns")
    private List<String> identifierColumns = Collections.emptyList();

    @JsonProperty("initial_load")
    private boolean initialLoad = true;

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getCatalog() {
        return catalog;
    }

    public List<String> getIdentifierColumns() {
        return identifierColumns;
    }

    public boolean isInitialLoad() {
        return initialLoad;
    }
}
