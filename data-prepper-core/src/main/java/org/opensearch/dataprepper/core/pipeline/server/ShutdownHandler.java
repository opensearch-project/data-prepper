/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.opensearch.dataprepper.DataPrepperShutdownOptions;
import org.opensearch.dataprepper.core.DataPrepper;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

/**
 * HttpHandler to handle requests to shut down the Data Prepper instance
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
            if(LOG.isInfoEnabled()) {
                LOG.info("Received HTTP shutdown request to shutdown Data Prepper. Shutdown pipelines and server. User-Agent='{}'",
                        exchange.getRequestHeaders().getFirst("User-Agent"));
            }
            final DataPrepperShutdownOptions dataPrepperShutdownOptions = mapShutdownOptions(exchange.getRequestURI());
            dataPrepper.shutdownPipelines(dataPrepperShutdownOptions);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
        } catch (final Exception e) {
            LOG.error("Caught exception shutting down data prepper", e);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
            dataPrepper.shutdownServers();
        }
    }

    private DataPrepperShutdownOptions mapShutdownOptions(final URI requestURI) {
        final List<NameValuePair> queryParams = URLEncodedUtils.parse(requestURI, Charset.defaultCharset());

        DataPrepperShutdownOptions.Builder shutdownOptionsBuilder
                = DataPrepperShutdownOptions.builder();

        for (final NameValuePair queryParam : queryParams) {
            final String value = queryParam.getValue();
            switch(queryParam.getName()) {
                case "bufferReadTimeout":
                    shutdownOptionsBuilder =
                            shutdownOptionsBuilder.withBufferReadTimeout(DataPrepperDurationParser.parse(value));
                    break;
                case "bufferDrainTimeout":
                    shutdownOptionsBuilder =
                            shutdownOptionsBuilder.withBufferDrainTimeout(DataPrepperDurationParser.parse(value));
                    break;
                default:
                    LOG.warn("Unrecognized query parameter '{}'", queryParam.getName());
            }
        }
        return shutdownOptionsBuilder.build();
    }
}
