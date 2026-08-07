/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientOptions;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Set;

public class HttpSinkSender {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkSender.class);
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(408, 429, 500, 502, 503, 504);
    private static final Set<Integer> AUTH_ERROR_CODES = Set.of(401, 403);

    private final AuthenticationDecorator authenticationDecorator;
    private final WebClient webClient;
    private final HttpSinkConfiguration config;
    private final int maxRetries;
    private final long retryIntervalMs;
    private final SinkMetrics sinkMetrics;

    public HttpSinkSender(final AuthenticationDecorator authenticationDecorator,
                          @Nonnull final HttpSinkConfiguration config,
                          final SinkMetrics sinkMetrics) {
        this.authenticationDecorator = authenticationDecorator;
        this.webClient = buildWebClient(config);
        this.config = config;
        this.sinkMetrics = sinkMetrics;
        this.maxRetries = config.getMaxUploadRetries();
        this.retryIntervalMs = config.getHttpRetryInterval().toMillis();
    }

    private static WebClient buildWebClient(final HttpSinkConfiguration config) {
        return WebClient.builder()
                .factory(ClientFactory.builder()
                        .connectTimeout(config.getConnectionTimeout())
                        .build())
                .options(ClientOptions.builder().build())
                .build();
    }

    public HttpEndpointResponse send(final byte[] payload) {
        HttpEndpointResponse response = null;
        int attempt = 0;

        while (attempt <= maxRetries) {
            try {
                final HttpRequest request = buildHttpRequest(payload);

                response = webClient.execute(request)
                        .aggregate()
                        .thenApply(resp -> {
                            int statusCode = resp.status().code();
                            String responseBody = resp.content().toStringUtf8();

                            return new HttpEndpointResponse(config.getUrl(), statusCode, responseBody);
                        })
                        .exceptionally(throwable -> {
                            LOG.error("Request failed", throwable);
                            return new HttpEndpointResponse(config.getUrl(), 0, throwable.getMessage());
                        })
                        .join();

                if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                    return response;
                }

                if (AUTH_ERROR_CODES.contains(response.getStatusCode())) {
                    LOG.error("Authentication error ({}), not retrying", response.getStatusCode());
                    return response;
                }

                if (!RETRYABLE_STATUS_CODES.contains(response.getStatusCode())) {
                    LOG.error("Non-retryable error ({}), message({}) not retrying", response.getStatusCode(), response.getErrMessage());
                    return response;
                }

                if (attempt < maxRetries) {
                    LOG.warn("Retryable error ({}), attempt {}/{}, retrying after {}ms",
                            response.getStatusCode(), attempt + 1, maxRetries, retryIntervalMs);
                    Thread.sleep(retryIntervalMs);
                    sinkMetrics.incrementRetries(1);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Retry interrupted", e);
                return new HttpEndpointResponse(config.getUrl(), 0, "Retry interrupted: " + e.getMessage());
            } catch (Exception e) {
                LOG.error("Failed to execute request, attempt {}/{}", attempt + 1, maxRetries + 1, e);
                if (attempt >= maxRetries) {
                    return new HttpEndpointResponse(config.getUrl(), 0, e.getMessage());
                }
                try {
                    Thread.sleep(retryIntervalMs);
                    sinkMetrics.incrementRetries(1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return new HttpEndpointResponse(config.getUrl(), 0, "Retry interrupted: " + ie.getMessage());
                }
            }
            attempt++;
        }

        return response != null ? response : new HttpEndpointResponse(config.getUrl(), 0, "Max retries exceeded");
    }

    private HttpRequest buildHttpRequest(final byte[] payload) {
        if (authenticationDecorator != null) {
            return authenticationDecorator.buildRequest(config.getUrl(), payload, config.getCustomHeaderOptions());
        }
        return buildPlainHttpRequest(payload);
    }

    private HttpRequest buildPlainHttpRequest(final byte[] payload) {
        final URI uri = URI.create(config.getUrl());
        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .scheme(uri.getScheme())
                .path(uri.getRawPath())
                .authority(uri.getAuthority());

        if (config.getCustomHeaderOptions() != null) {
            config.getCustomHeaderOptions().forEach((key, values) ->
                    values.forEach(value -> headersBuilder.add(key, value))
            );
        }

        return HttpRequest.of(headersBuilder.build(), HttpData.wrap(payload));
    }
}
