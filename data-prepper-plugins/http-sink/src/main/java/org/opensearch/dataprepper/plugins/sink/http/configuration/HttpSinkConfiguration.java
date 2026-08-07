/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
    static final boolean DEFAULT_INSECURE = false;
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

    @JsonProperty("insecure")
    private boolean insecure = DEFAULT_INSECURE;

    @JsonProperty("insecure_skip_verify")
    private boolean insecureSkipVerify = DEFAULT_INSECURE;

    @JsonProperty("connection_timeout")
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @JsonProperty("aws_sigv4_service_name")
    private String awsSigv4ServiceName;

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

    public String getAwsSigv4ServiceName() {
        return awsSigv4ServiceName;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @Valid
    public boolean isValidConfig() {
        return (insecure && awsConfig == null) || (!insecure && awsConfig != null);
    }

}
