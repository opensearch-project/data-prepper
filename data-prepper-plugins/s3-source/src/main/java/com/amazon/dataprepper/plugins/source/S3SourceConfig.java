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
    private PluginModel codec;

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

    @JsonProperty("on_error")
    private OnErrorOption onErrorOption = OnErrorOption.RETAIN_MESSAGES;

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

    public AwsAuthenticationOptions getAWSAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public OnErrorOption getOnErrorOption() {
        return onErrorOption;
    }
}
