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

    private static final Long DEFAULT_PROVISIONED_READ_CAPACITY_UNITS = 10L;
    private static final Long DEFAULT_PROVISIONED_WRITE_CAPACITY_UNITS = 10L;

    private final String tableName;
    private final String region;
    private final String stsRoleArn;

    private Long provisionedReadCapacityUnits = DEFAULT_PROVISIONED_READ_CAPACITY_UNITS;
    private Long provisionedWriteCapacityUnits = DEFAULT_PROVISIONED_WRITE_CAPACITY_UNITS;

    @JsonCreator
    public DynamoStoreSettings(@JsonProperty("table_name") final String tableName,
                               @JsonProperty("region") final String region,
                               @JsonProperty("sts_role_arn") final String stsRoleArn,
                               @JsonProperty("provisioned_read_capacity_units") final Long provisionedReadCapacityUnits,
                               @JsonProperty("provisioned_write_capacity_units") final Long provisionedWriteCapacityUnits) {
        Objects.requireNonNull(tableName, "table_name is required for dynamo store settings");
        Objects.requireNonNull(region, "region is required for dynamo store settings");
        Objects.requireNonNull(stsRoleArn, "sts_role_arn is required for dynamo store settings");

        this.tableName = tableName;
        this.region = region;
        this.stsRoleArn = stsRoleArn;

        if (Objects.nonNull(provisionedReadCapacityUnits)) {
            this.provisionedReadCapacityUnits = provisionedReadCapacityUnits;
        }

        if (Objects.nonNull(provisionedWriteCapacityUnits)) {
            this.provisionedWriteCapacityUnits = provisionedWriteCapacityUnits;
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getRegion() {
        return region;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public Long getProvisionedReadCapacityUnits() {
        return provisionedReadCapacityUnits;
    }

    public Long getProvisionedWriteCapacityUnits() {
        return provisionedWriteCapacityUnits;
    }
}
