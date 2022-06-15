/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.plugins.source.configuration.NotificationTypeOption;
import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.OnErrorOption;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public class S3SourceConfig {
    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);
    static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;

    @JsonProperty("notification_type")
    @NotNull
    private NotificationTypeOption notificationType;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("sqs")
    @NotNull
    private SqsOptions sqsOptions;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("on_error")
    private OnErrorOption onErrorOption = OnErrorOption.RETAIN_MESSAGES;

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_BUFFER_TIMEOUT;

    @JsonProperty("records_to_accumulate")
    private int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;

    public NotificationTypeOption getNotificationType() {
        return notificationType;
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
}
