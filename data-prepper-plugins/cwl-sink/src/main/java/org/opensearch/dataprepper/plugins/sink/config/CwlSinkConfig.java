package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class CwlSinkConfig {
    public static final String DEFAULT_BUFFER_TYPE = "in_memory";

    //TODO: Change this to custom aws config class as its just a data container.
    @JsonProperty("aws_config")
    @NotNull
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("threshold_config")
    @NotNull
    private ThresholdConfig thresholdConfig;

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
