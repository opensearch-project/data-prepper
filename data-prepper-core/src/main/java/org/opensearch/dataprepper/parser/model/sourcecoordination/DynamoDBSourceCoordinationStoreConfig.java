/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model.sourcecoordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * the configuration for utilizing DynamoDB as the distributed store for {@link org.opensearch.dataprepper.model.source.SourceCoordinator}
 * @since 2.2
 */
public class DynamoDBSourceCoordinationStoreConfig implements SourceCoordinationStoreConfig {

    private static final String DYNAMO_DB = "dynamodb";

    private final DynamoStoreSettings dynamoStoreSettings;

    @JsonCreator
    public DynamoDBSourceCoordinationStoreConfig(
            @JsonProperty(DYNAMO_DB)
            final DynamoStoreSettings dynamoStoreSettings) {
        Objects.requireNonNull(dynamoStoreSettings, "source_coordination store settings for dynamodb must not be null");
        this.dynamoStoreSettings = dynamoStoreSettings;
    }

    @Override
    public String getStoreName() {
        return DYNAMO_DB;
    }

    @Override
    public DynamoStoreSettings getStoreSettings() {
        return dynamoStoreSettings;
    }
}
