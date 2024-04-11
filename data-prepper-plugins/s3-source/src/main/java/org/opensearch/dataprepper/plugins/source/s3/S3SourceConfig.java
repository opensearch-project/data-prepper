/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import org.opensearch.dataprepper.aws.validator.AwsAccountId;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationSourceOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationTypeOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.SqsOptions;

import java.time.Duration;
import java.util.Map;

public class S3SourceConfig {
    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);
    static final int DEFAULT_NUMBER_OF_WORKERS = 1;
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;
    static final String DEFAULT_METADATA_ROOT_KEY = "s3/";

    @JsonProperty("notification_type")
    private NotificationTypeOption notificationType;

    @JsonProperty("notification_source")
    private NotificationSourceOption notificationSource = NotificationSourceOption.S3;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("sqs")
    @Valid
    private SqsOptions sqsOptions;

    @JsonProperty("workers")
    @Min(1)
    @Max(1000)
    @Valid
    private int numWorkers = DEFAULT_NUMBER_OF_WORKERS;

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

    @JsonProperty("bucket_owners")
    private Map<String, @AwsAccountId String> bucketOwners;

    @JsonProperty("default_bucket_owner")
    @AwsAccountId
    private String defaultBucketOwner;

    @JsonProperty("metadata_root_key")
    private String metadataRootKey = DEFAULT_METADATA_ROOT_KEY;
    @JsonProperty("s3_select")
    @Valid
    private S3SelectOptions s3SelectOptions;

    @JsonProperty("scan")
    @Valid
    private S3ScanScanOptions s3ScanScanOptions;

    @JsonProperty("delete_s3_objects_on_read")
    private boolean deleteS3ObjectsOnRead = false;

    @AssertTrue(message = "A codec is required for reading objects.")
    boolean isCodecProvidedWhenNeeded() {
        if(s3SelectOptions == null)
            return codec != null;
        return true;
    }

    public NotificationTypeOption getNotificationType() {
        return notificationType;
    }

    public NotificationSourceOption getNotificationSource() {
        return notificationSource;
    }

    boolean getAcknowledgements() {
        return acknowledgments;
    }

    public int getNumWorkers() {
        return numWorkers;
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

    public boolean isDeleteS3ObjectsOnRead() {
        return deleteS3ObjectsOnRead;
    }

    public Map<String, String> getBucketOwners() {
        return bucketOwners;
    }

    public String getDefaultBucketOwner() {
        return defaultBucketOwner;
    }
}
