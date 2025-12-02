/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.util;

import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.utils.AttributeMap;

import java.time.Duration;

public class SdkHttpClientExecutor implements HttpClientExecutor {
    private final SdkHttpClient httpClient;

    // Configuration constants
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
    private static final int DEFAULT_MAX_CONNECTIONS = 10;

    public SdkHttpClientExecutor() {
        this(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_MAX_CONNECTIONS);
    }

    /**
     * Constructor with configurable timeouts for flexibility
     * @param connectionTimeout timeout for establishing connection
     * @param readTimeout timeout for reading response data
     * @param maxConnections maximum number of connections
     */
    public SdkHttpClientExecutor(Duration connectionTimeout, Duration readTimeout, int maxConnections) {
        AttributeMap attributeMap = AttributeMap.builder()
                .put(SdkHttpConfigurationOption.CONNECTION_TIMEOUT, connectionTimeout)
                .put(SdkHttpConfigurationOption.READ_TIMEOUT, readTimeout)
                .put(SdkHttpConfigurationOption.MAX_CONNECTIONS, maxConnections)
                .build();
        this.httpClient = new DefaultSdkHttpClientBuilder().buildWithDefaults(attributeMap);
    }

    @Override
    public HttpExecuteResponse execute(HttpExecuteRequest executeRequest) throws Exception {
        return httpClient.prepareRequest(executeRequest).call();
    }
}
