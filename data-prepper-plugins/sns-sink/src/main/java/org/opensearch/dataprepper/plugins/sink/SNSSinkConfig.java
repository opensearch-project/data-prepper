/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;

/**
 * sns sink configuration class contains properties, used to read yaml configuration.
 */
public class SNSSinkConfig {

    private static final int DEFAULT_CONNECTION_RETRIES = 5;
    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("topic")
    @NotNull
    @NotEmpty
    private String topicArn;

    @JsonProperty("id")
    private String id;

    @JsonProperty("threshold")
    @NotNull
    private ThresholdOptions thresholdOptions;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("dlq_file")
    private String dlqFile;

    @JsonProperty("buffer_type")
    private BufferTypeOptions bufferType = BufferTypeOptions.IN_MEMORY;

    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    public PluginModel getDlq() {
        return dlq;
    }

    public String getDlqFile() {
        return dlqFile;
    }

    /**
     * Aws Authentication configuration Options.
     * @return aws authentication options.
     */
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    /**
     * Threshold configuration Options.
     * @return threshold option object.
     */
    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    public String getTopicArn() {
        return topicArn;
    }

    public String getId() {
        return id;
    }

    /**
     * Sink codec configuration Options.
     * @return  codec plugin model.
     */
    public PluginModel getCodec() {
        return codec;
    }

    /**
     * Buffer type configuration Options.
     * @return buffer type option object.
     */
    public BufferTypeOptions getBufferType() {
        return bufferType;
    }

    /**
     * SNS client connection retries configuration Options.
     * @return max connection retries value.
     */
    public int getMaxConnectionRetries() {
        return maxConnectionRetries;
    }

    /**
     * SNS object upload retries configuration Options.
     * @return maximum upload retries value.
     */
    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }
}