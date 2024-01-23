/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.opensearch.dataprepper.DataPrepper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * HttpHandler to handle requests to shut down the data prepper instance
 */
public class ShutdownHandler implements HttpHandler {
    private final DataPrepper dataPrepper;
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownHandler.class);

    public ShutdownHandler(final DataPrepper dataPrepper) {
        this.dataPrepper = dataPrepper;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        final String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equals(HttpMethod.POST)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            exchange.getResponseBody().close();
            return;
        }

        try {
            LOG.info("Received HTTP shutdown request to shutdown Data Prepper. Shutdown pipelines and server.");
            dataPrepper.shutdownPipelines();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
        } catch (final Exception e) {
            LOG.error("Caught exception shutting down data prepper", e);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
            dataPrepper.shutdownServers();
        }
    }
}
