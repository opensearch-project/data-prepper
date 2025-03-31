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

    public SdkHttpClientExecutor() {
        AttributeMap attributeMap = AttributeMap.builder()
                .put(SdkHttpConfigurationOption.CONNECTION_TIMEOUT, Duration.ofMillis(30000))
                .put(SdkHttpConfigurationOption.READ_TIMEOUT, Duration.ofMillis(3000))
                .put(SdkHttpConfigurationOption.MAX_CONNECTIONS, 10)
                .build();
        this.httpClient = new DefaultSdkHttpClientBuilder().buildWithDefaults(attributeMap);
    }

    @Override
    public HttpExecuteResponse execute(HttpExecuteRequest executeRequest) throws Exception {
        return httpClient.prepareRequest(executeRequest).call();
    }
}
