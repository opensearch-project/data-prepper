/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.source.configuration.NotificationTypeOption;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.configuration.SqsOptions;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public class S3SourceConfig {
    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;
    static final String DEFAULT_METADATA_ROOT_KEY = "s3/";

    @JsonProperty("notification_type")
    private NotificationTypeOption notificationType;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("sqs")
    private SqsOptions sqsOptions;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("on_error")
    private OnErrorOption onErrorOption = OnErrorOption.RETAIN_MESSAGES;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_BUFFER_TIMEOUT;

    @JsonProperty("records_to_accumulate")
    private int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

    @JsonProperty("disable_bucket_ownership_validation")
    private boolean disableBucketOwnershipValidation = false;

    @JsonProperty("metadata_root_key")
    private String metadataRootKey = DEFAULT_METADATA_ROOT_KEY;
    @JsonProperty("s3_select")
    private S3SelectOptions s3SelectOptions;

    @JsonProperty("scan")
    private S3ScanScanOptions s3ScanScanOptions;

    @AssertTrue(message = "A codec is required for reading objects.")
    boolean isCodecProvidedWhenNeeded() {
        if(s3SelectOptions == null)
            return codec != null;
        return true;
    }

    public NotificationTypeOption getNotificationType() {
        return notificationType;
    }

    boolean getAcknowledgements() {
        return acknowledgments;
    }

    public CompressionOption getCompression() {
        return compression;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public SqsOptions getSqsOptions() {
        return sqsOptions;
    }

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public OnErrorOption getOnErrorOption() {
        return onErrorOption;
    }

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    public int getNumberOfRecordsToAccumulate() {
        return numberOfRecordsToAccumulate;
    }

    public boolean isDisableBucketOwnershipValidation() {
        return disableBucketOwnershipValidation;
    }

    public String getMetadataRootKey() {
        return metadataRootKey;
    }

    public S3SelectOptions getS3SelectOptions() {
        return s3SelectOptions;
    }

    public S3ScanScanOptions getS3ScanScanOptions() {
        return s3ScanScanOptions;
    }

}
