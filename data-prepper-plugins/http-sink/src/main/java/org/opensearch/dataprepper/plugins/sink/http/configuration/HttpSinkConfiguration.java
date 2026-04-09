/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.aws.api.AwsConfig;

import java.time.Duration;
import java.util.Map;
import java.util.List;

public class HttpSinkConfiguration {

    static final int DEFAULT_UPLOAD_RETRIES = 3;
    public static final Duration DEFAULT_HTTP_RETRY_INTERVAL = Duration.ofSeconds(30);
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);

    @NotNull
    @JsonProperty("url")
    private String url;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("threshold")
    private ThresholdOptions thresholdOptions;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("http_retry_interval")
    private Duration httpRetryInterval = DEFAULT_HTTP_RETRY_INTERVAL;

    @JsonProperty("request_timeout")
    private Duration requestTimeout;

    @JsonProperty("connection_timeout")
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @JsonProperty("custom_headers")
    private Map<String, List<String>> customHeaderOptions;

    public String getUrl() {
        return url;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public ThresholdOptions getThresholdOptions() {
        if (thresholdOptions == null) {
            return new ThresholdOptions();
        }
        return thresholdOptions;
    }

    public Map<String, List<String>> getCustomHeaderOptions() {
        return customHeaderOptions;
    }

    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public Duration getHttpRetryInterval() {
        return httpRetryInterval;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

}
