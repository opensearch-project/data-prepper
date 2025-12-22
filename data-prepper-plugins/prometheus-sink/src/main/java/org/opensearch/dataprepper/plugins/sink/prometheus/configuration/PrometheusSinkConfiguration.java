 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */
package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.time.Duration;

import static com.linecorp.armeria.common.MediaTypeNames.X_PROTOBUF;

public class PrometheusSinkConfiguration {

    static final int DEFAULT_MAX_RETRIES = 5;

    public static final CompressionOption DEFAULT_ENCODING = CompressionOption.SNAPPY;

    private static final String DEFAULT_CONTENT_TYPE = X_PROTOBUF;

    static final String DEFAULT_REMOTE_WRITE_VERSION = "0.1.0";
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_OUT_OF_ORDER_WINDOW = Duration.ofSeconds(5);

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsConfig awsConfig;

    @NotNull
    @JsonProperty("url")
    private String url;

    @JsonProperty("out_of_order_window")
    private Duration outOfOrderWindow = DEFAULT_OUT_OF_ORDER_WINDOW;

    @JsonProperty("max_retries")
    private int maxRetries = DEFAULT_MAX_RETRIES;

    @JsonProperty("threshold")
    private PrometheusSinkThresholdConfig thresholdConfig;

    @JsonProperty("encoding")
    private CompressionOption encoding = DEFAULT_ENCODING;

    @JsonProperty("content_type")
    private String contentType = DEFAULT_CONTENT_TYPE;

    @JsonProperty("remote_write_version")
    private String remoteWriteVersion = DEFAULT_REMOTE_WRITE_VERSION;

    @JsonProperty("request_timeout")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 600)
    private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;

    @JsonProperty("connection_timeout")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 600)
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @JsonProperty("idle_timeout")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 600)
    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    @JsonProperty("sanitize_names")
    private boolean sanitizeNames = true;

    public boolean getSanitizeNames() {
        return sanitizeNames;
    }

    public String getUrl() {
        return url;
    }

    public PrometheusSinkThresholdConfig getThresholdConfig() {
        if (thresholdConfig == null) {
            return new PrometheusSinkThresholdConfig();
        }
        return thresholdConfig;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public CompressionOption getEncoding() {
        return encoding;
    }

    public String getContentType() {
        return contentType;
    }

    public Duration getOutOfOrderWindow() {
        return outOfOrderWindow;
    }

    public String getRemoteWriteVersion() {
        return remoteWriteVersion;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    @AssertTrue(message = "encoding or content_type or remote_write_version is incorrect.")
    boolean isValidConfig() {
        return  url.startsWith("https://") &&
                encoding == CompressionOption.SNAPPY &&
                contentType.equals(X_PROTOBUF) &&
                remoteWriteVersion.equals(DEFAULT_REMOTE_WRITE_VERSION);
    }
}
