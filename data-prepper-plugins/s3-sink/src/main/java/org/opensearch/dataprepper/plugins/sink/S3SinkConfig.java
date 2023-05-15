/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * s3 sink configuration class contains properties, used to read yaml configuration.
 */
public class S3SinkConfig {

    private static final int DEFAULT_CONNECTION_RETRIES = 5;
    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("bucket")
    @NotNull
    @Valid
    private BucketOptions bucketOptions;

    @JsonProperty("threshold")
    @NotNull
    private ThresholdOptions thresholdOptions;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("buffer_type")
    private BufferTypeOptions bufferType = BufferTypeOptions.INMEMORY;

    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

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

    /**
     * S3 bucket configuration Options.
     * @return  bucket option object.
     */
    public BucketOptions getBucketOptions() {
        return bucketOptions;
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
     * S3 client connection retries configuration Options.
     * @return max connection retries value.
     */
    public int getMaxConnectionRetries() {
        return maxConnectionRetries;
    }

    /**
     * S3 object upload retries configuration Options.
     * @return maximum upload retries value.
     */
    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }
}