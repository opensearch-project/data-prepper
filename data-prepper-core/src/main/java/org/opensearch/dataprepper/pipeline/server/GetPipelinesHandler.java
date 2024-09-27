/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.pipeline.PipelinesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

public class GetPipelinesHandler implements HttpHandler {

    private final PipelinesProvider pipelinesProvider;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Logger LOG = LoggerFactory.getLogger(GetPipelinesHandler.class);

    public GetPipelinesHandler(final PipelinesProvider pipelinesProvider) {
        this.pipelinesProvider = pipelinesProvider;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equals(HttpMethod.GET)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            exchange.getResponseBody().close();
            return;
        }

        try {
            List<PipelineModel> pipelineModels = new ArrayList<>(pipelinesProvider.getPipelinesDataFlowModel().getPipelines().values());

            final byte[] response = OBJECT_MAPPER.writeValueAsString(pipelineModels).getBytes();

            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            exchange.getResponseBody().write(response);
        } catch (final Exception e) {
            LOG.error("Caught exception listing pipelines", e);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
        }
    }
}
