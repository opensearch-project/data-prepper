/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

import software.amazon.awssdk.arns.Arn;

public class TableInfo {

    private final String tableArn;
    private final String tableName;

    private final TableMetadata metadata;

    public TableInfo(String tableArn, TableMetadata metadata) {
        this.tableArn = tableArn;
        this.metadata = metadata;
        this.tableName = Arn.fromString(tableArn).resource().resource();
    }

    public String getTableArn() {
        return tableArn;
    }

    public String getTableName() { return tableName; }

    public TableMetadata getMetadata() {
        return metadata;
    }

}
