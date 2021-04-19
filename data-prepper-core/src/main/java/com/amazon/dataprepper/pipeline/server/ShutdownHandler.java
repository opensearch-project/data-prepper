/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import java.io.IOException;
import java.net.HttpURLConnection;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpHandler to handle requests to shut down the data prepper instance
 */
public class ShutdownHandler implements HttpHandler {

    private final DataPrepper dataPrepper;
    private final static Logger LOG = LoggerFactory.getLogger(ShutdownHandler.class);

    public ShutdownHandler(final DataPrepper dataPrepper) {
        this.dataPrepper = dataPrepper;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            dataPrepper.shutdown();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
        } catch (Exception e) {
            LOG.error("Caught exception shutting down data prepper", e);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
            dataPrepper.shutdownDataPrepperServer();
        }
    }
}
