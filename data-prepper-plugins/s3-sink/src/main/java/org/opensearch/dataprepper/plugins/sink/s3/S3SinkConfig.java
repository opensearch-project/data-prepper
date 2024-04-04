/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AggregateThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ThresholdOptions;

/**
 * s3 sink configuration class contains properties, used to read yaml configuration.
 */
public class S3SinkConfig {
    static final String S3_PREFIX = "s3://";

    private static final int DEFAULT_CONNECTION_RETRIES = 5;
    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("bucket")
    @NotEmpty
    @Size(min = 3, max = 500, message = "bucket length should be at least 3 characters")
    private String bucketName;

    @JsonProperty("object_key")
    @Valid
    private ObjectKeyOptions objectKeyOptions = new ObjectKeyOptions();

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("threshold")
    @NotNull
    @Valid
    private ThresholdOptions thresholdOptions;

    @JsonProperty("aggregate_threshold")
    @Valid
    private AggregateThresholdOptions aggregateThresholdOptions;

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
     * Threshold configuration Options at the individual S3 group level
     * @return threshold option object.
     */
    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    /**
     * Threshold configuration for the aggregation of all S3 groups
     */
    public AggregateThresholdOptions getAggregateThresholdOptions() { return aggregateThresholdOptions; }

    /**
     * Read s3 bucket name configuration.
     * @return bucket name.
     */
    public String getBucketName() {
        if (bucketName.startsWith(S3_PREFIX)) {
            return bucketName.substring(S3_PREFIX.length());
        }
        return bucketName;
    }

    /**
     * S3 {@link ObjectKeyOptions} configuration Options.
     * @return object key options.
     */
    public ObjectKeyOptions getObjectKeyOptions() {
        if (objectKeyOptions == null) {
            objectKeyOptions = new ObjectKeyOptions();
        }
        return objectKeyOptions;
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

    public CompressionOption getCompression() {
        return compression;
    }
}