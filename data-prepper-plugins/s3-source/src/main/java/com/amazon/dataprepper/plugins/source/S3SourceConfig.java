/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.CodecOption;
import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import com.amazon.dataprepper.plugins.source.configuration.NotificationTypeOption;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
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

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthentication;

    public NotificationTypeOption getNotificationType() {
        return notificationType;
    }

    public CompressionOption getCompression() {
        return compression;
    }

    public CodecOption getCodec() {
        return codec;
    }

    public AwsAuthenticationOptions getAWSAuthentication() {
        return awsAuthentication;
    }
}
