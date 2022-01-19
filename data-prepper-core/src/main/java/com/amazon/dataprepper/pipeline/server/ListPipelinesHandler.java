/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.core.DataPrepper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * HttpHandler to handle requests for listing pipelines running on the data prepper instance
 */
public class ListPipelinesHandler implements HttpHandler {

    private final DataPrepper dataPrepper;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Logger LOG = LoggerFactory.getLogger(ListPipelinesHandler.class);

    public ListPipelinesHandler(final DataPrepper dataPrepper) {
        this.dataPrepper = dataPrepper;
    }

    private static class PipelineListing {
        public String name;

        public PipelineListing(final String name) {
            this.name = name;
        }
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            List<PipelineListing> pipelines = dataPrepper.getTransformationPipelines()
                    .entrySet()
                    .stream()
                    .map(entry -> new PipelineListing(entry.getKey()))
                    .collect(Collectors.toList());
            final byte[] response = OBJECT_MAPPER.writeValueAsString(Collections.singletonMap("pipelines", pipelines)).getBytes();
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            exchange.getResponseBody().write(response);
        } catch (Exception e) {
            LOG.error("Caught exception listing pipelines", e);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
        }
    }
}
