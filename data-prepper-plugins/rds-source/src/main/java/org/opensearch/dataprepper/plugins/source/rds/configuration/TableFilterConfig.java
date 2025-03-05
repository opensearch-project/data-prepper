/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class TableFilterConfig {

    @JsonProperty("database")
    @NotEmpty
    private String database;

    @JsonProperty("include")
    @Size(max = 1000, message = "Table filter list should not be more than 1000")
    private List<String> include = Collections.emptyList();

    @JsonProperty("exclude")
    @Size(max = 1000, message = "Table filter list should not be more than 1000")
    private List<String> exclude = Collections.emptyList();

    /**
     * This method applies the table filter configuration to the given set of table names.
     *
     * @param tableNames        The set of table names to be filtered
     */
    public void applyTableFilter(Set<String> tableNames) {
        if (!getInclude().isEmpty()) {
            List<String> includeTableList = getInclude().stream()
                    .map(item -> getDatabase() + "." + item)
                    .collect(Collectors.toList());
            tableNames.retainAll(includeTableList);
        }

        if (!getExclude().isEmpty()) {
            List<String> excludeTableList = getExclude().stream()
                    .map(item -> getDatabase() + "." + item)
                    .collect(Collectors.toList());
            excludeTableList.forEach(tableNames::remove);
        }
    }
}
