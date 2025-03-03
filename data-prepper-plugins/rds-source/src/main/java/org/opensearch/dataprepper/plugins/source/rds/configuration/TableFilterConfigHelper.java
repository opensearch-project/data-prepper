/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import java.util.Set;

public class TableFilterConfigHelper {

    /**
     * This method applies the table filter configuration to the given set of table names.
     *
     * @param tableNames        The set of table names to be filtered
     * @param tableFilterConfig The table filter configuration to be applied
     */
    public static void applyTableFilter(Set<String> tableNames, TableFilterConfig tableFilterConfig) {
        if (!tableFilterConfig.getInclude().isEmpty()) {
            tableNames.retainAll(tableFilterConfig.getInclude());
        }
        if (!tableFilterConfig.getExclude().isEmpty()) {
            tableFilterConfig.getExclude().forEach(tableNames::remove);
        }
    }
}
