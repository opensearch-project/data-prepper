/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.HashMap;
import java.util.Map;

public class CloudWatchLogsSinkConfig {
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final int DEFAULT_NUM_WORKERS = 10;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("threshold")
    @Valid
    private ThresholdConfig thresholdConfig = new ThresholdConfig();

    @JsonProperty("log_group")
    @NotEmpty
    @NotNull
    private String logGroup;

    @JsonProperty("log_stream")
    @NotEmpty
    @NotNull
    private String logStream;

    @JsonProperty(value = "max_retries", defaultValue = "5")
    @Min(1)
    @Max(15)
    private int maxRetries = DEFAULT_RETRY_COUNT;

    @JsonProperty(value = "workers", defaultValue = "10")
    @Min(1)
    @Max(50)
    private int workers = DEFAULT_NUM_WORKERS;

    @JsonProperty("header_overrides")
    @Size(max = 10, message = "Maximum 10 custom headers allowed")
    @ValidCustomHeaders
    private Map<String, String> headerOverrides = new HashMap<>();

    @JsonProperty("endpoint")
    private String endpoint;

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public ThresholdConfig getThresholdConfig() {
        return thresholdConfig;
    }

    public PluginModel getDlq() {
        return dlq;
    }

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getWorkers() {
        return workers;
    }

    public Map<String, String> getHeaderOverrides() {
        return headerOverrides;
    }

    public String getEndpoint() {
        return endpoint;
    }

}
