/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HttpHandler to handle readiness check requests.
 * Returns 200 OK when all pipeline sources have started and are ready to receive traffic.
 * Returns 503 Service Unavailable when any pipeline source has not yet started.
 *
 * This endpoint is designed to be used as an ALB health check target to prevent
 * routing traffic to targets before their data ingestion ports are listening.
 */
public class ReadinessHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReadinessHandler.class);
    private final PipelinesProvider pipelinesProvider;

    public ReadinessHandler(final PipelinesProvider pipelinesProvider) {
        this.pipelinesProvider = pipelinesProvider;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        final String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equals(HttpMethod.GET)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            exchange.getResponseBody().close();
            return;
        }

        try {
            final Map<String, Pipeline> pipelines = pipelinesProvider.getTransformationPipelines();

            if (pipelines.isEmpty()) {
                LOG.debug("Readiness check: no pipelines registered yet");
                final byte[] response = "{\"ready\":false,\"reason\":\"no pipelines registered\"}".getBytes();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, response.length);
                exchange.getResponseBody().write(response);
                return;
            }

            final boolean allReady = pipelines.values().stream()
                    .allMatch(Pipeline::isSourceStarted);

            if (allReady) {
                LOG.debug("Readiness check: all {} pipelines ready", pipelines.size());
                final byte[] response = "{\"ready\":true}".getBytes();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
                exchange.getResponseBody().write(response);
            } else {
                final String notReady = pipelines.entrySet().stream()
                        .filter(e -> !e.getValue().isSourceStarted())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.joining(", "));
                LOG.debug("Readiness check: pipelines not ready: [{}]", notReady);
                final byte[] response = String.format(
                        "{\"ready\":false,\"reason\":\"pipelines not started: [%s]\"}", notReady).getBytes();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, response.length);
                exchange.getResponseBody().write(response);
            }
        } catch (final Exception e) {
            LOG.error("Caught exception during readiness check", e);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
        }
    }
}
