/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;

import java.util.List;

/**
 * Configuration for DynamoDB Source
 */
public class DynamoDBSourceConfig {

    @JsonProperty("tables")
    private List<TableConfig> tableConfigs;


    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    
    @JsonProperty("coordinator")
    private PluginModel coordinationStoreConfig;


    public DynamoDBSourceConfig() {
    }


    public List<TableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public AwsAuthenticationConfig getAwsAuthenticationConfig() {
        return awsAuthenticationConfig;
    }

    public PluginModel getCoordinationStoreConfig() {
        return coordinationStoreConfig;
    }

}
