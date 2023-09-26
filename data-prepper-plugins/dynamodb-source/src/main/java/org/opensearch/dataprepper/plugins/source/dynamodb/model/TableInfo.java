/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

public class TableInfo {

    private final String tableArn;

    private final TableMetadata metadata;

    public TableInfo(String tableArn, TableMetadata metadata) {
        this.tableArn = tableArn;
        this.metadata = metadata;
    }

    public String getTableArn() {
        return tableArn;
    }

    public TableMetadata getMetadata() {
        return metadata;
    }

}
