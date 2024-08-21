/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class TableConfig {

    @JsonProperty("table_arn")
    @NotNull
    @NotEmpty(message = "DynamoDB Table ARN cannot be null or empty")
    private String tableArn;

    @JsonProperty("export")
    @Valid
    private ExportConfig exportConfig;

    @JsonProperty(value = "stream")
    @Valid
    private StreamConfig streamConfig;


    public String getTableArn() {
        return tableArn;
    }

    public ExportConfig getExportConfig() {
        return exportConfig;
    }

    public StreamConfig getStreamConfig() {
        return streamConfig;
    }
}
