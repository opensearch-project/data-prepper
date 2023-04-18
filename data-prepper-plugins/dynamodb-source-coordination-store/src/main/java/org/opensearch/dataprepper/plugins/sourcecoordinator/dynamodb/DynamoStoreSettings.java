/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * The configuration settings for a {@link org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbSourceCoordinationStore}
 * @since 2.2
 */
public class DynamoStoreSettings {

    private final String tableName;

    @JsonCreator
    public DynamoStoreSettings(@JsonProperty("table_name") final String tableName) {
        Objects.requireNonNull(tableName, "table_name is required for dynamo store settings");
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
