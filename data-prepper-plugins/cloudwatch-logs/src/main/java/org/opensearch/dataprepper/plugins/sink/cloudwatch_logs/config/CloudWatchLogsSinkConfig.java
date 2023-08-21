/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class CloudWatchLogsSinkConfig {
    public static final String DEFAULT_BUFFER_TYPE = "in_memory";

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("threshold")
    private ThresholdConfig thresholdConfig = new ThresholdConfig();

    @JsonProperty("buffer_type")
    private String bufferType = DEFAULT_BUFFER_TYPE;

    @JsonProperty("log_group")
    @NotEmpty
    @NotNull
    private String logGroup;

    @JsonProperty("log_stream")
    @NotEmpty
    @NotNull
    private String logStream;

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public ThresholdConfig getThresholdConfig() {
        return thresholdConfig;
    }

    public String getBufferType() {
        return bufferType;
    }

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }
}
