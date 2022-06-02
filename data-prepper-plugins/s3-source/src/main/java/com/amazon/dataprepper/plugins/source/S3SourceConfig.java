/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.NotificationTypeOption;
import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import com.amazon.dataprepper.plugins.source.configuration.CodecOption;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public class S3SourceConfig {

    @JsonProperty("notification_type")
    @NotNull
    private NotificationTypeOption notificationType;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("codec")
    @NotNull
    @Valid
    private CodecOption codec;

    @JsonProperty("sqs")
    @NotNull
    private SqsOptions sqsOptions;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("thread_count")
    @Min(0)
    private int threadCount;

    public NotificationTypeOption getNotificationType() {
        return notificationType;
    }

    public CompressionOption getCompression() {
        return compression;
    }

    public CodecOption getCodec() {
        return codec;
    }

    public SqsOptions getSqsOptions() {
        return sqsOptions;
    }

    public AwsAuthenticationOptions getAWSAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public int getThreadCount() {
        return threadCount;
    }
}
