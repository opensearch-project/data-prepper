/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TableFilterConfigHelper {

    /**
     * This method applies the table filter configuration to the given set of table names.
     *
     * @param tableNames        The set of table names to be filtered
     * @param tableFilterConfig The table filter configuration to be applied
     */
    public static void applyTableFilter(Set<String> tableNames, TableFilterConfig tableFilterConfig) {
        if (!tableFilterConfig.getInclude().isEmpty()) {
            List<String> includeTableList = tableFilterConfig.getInclude().stream()
                    .map(item -> tableFilterConfig.getDatabase() + "." + item)
                    .collect(Collectors.toList());
            tableNames.retainAll(includeTableList);
        }

        if (!tableFilterConfig.getExclude().isEmpty()) {
            List<String> excludeTableList = tableFilterConfig.getExclude().stream()
                    .map(item -> tableFilterConfig.getDatabase() + "." + item)
                    .collect(Collectors.toList());
            excludeTableList.forEach(tableNames::remove);
        }
    }
}
