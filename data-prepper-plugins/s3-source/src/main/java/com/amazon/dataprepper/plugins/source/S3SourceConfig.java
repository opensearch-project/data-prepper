/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AWSAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.CodecOption;
import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import com.amazon.dataprepper.plugins.source.configuration.NotificationTypeOption;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;

public class S3SourceConfig {

    @JsonProperty("notification_type")
    private NotificationTypeOption notificationType;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty("codec")
    @Valid
    private CodecOption codec;

    @JsonProperty("aws")
    @Valid
    private AWSAuthenticationOptions awsAuthentication;

    public NotificationTypeOption getNotificationType() {
        return notificationType;
    }

    public CompressionOption getCompression() {
        return compression;
    }

    public CodecOption getCodec() {
        return codec;
    }

    public AWSAuthenticationOptions getAWSAuthentication() {
        return awsAuthentication;
    }

    @AssertTrue(message = "notification_type cannot be null or empty")
    boolean isNotificationTypeValid() {
        return (notificationType != null);
    }
}
