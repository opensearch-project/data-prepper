/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for DynamoDB Source
 */
public class DynamoDBSourceConfig {

    @JsonProperty("tables")
    private List<TableConfig> tableConfigs = Collections.emptyList();

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("shard_acknowledgment_timeout")
    private Duration shardAcknowledgmentTimeout = Duration.ofMinutes(10);

    @JsonProperty("s3_data_file_acknowledgment_timeout")
    private Duration dataFileAcknowledgmentTimeout = Duration.ofMinutes(5);

    public DynamoDBSourceConfig() {
    }


    public List<TableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public AwsAuthenticationConfig getAwsAuthenticationConfig() {
        return awsAuthenticationConfig;
    }

    public boolean isAcknowledgmentsEnabled() {
        return acknowledgments;
    }

    public Duration getShardAcknowledgmentTimeout() {
        return shardAcknowledgmentTimeout;
    }

    public Duration getDataFileAcknowledgmentTimeout() { return dataFileAcknowledgmentTimeout; }

    @AssertTrue(message = "Exactly one table must be configured for the DynamoDb source.")
    boolean isExactlyOneTableConfigured() {
        return tableConfigs.size() == 1;
    }
}
