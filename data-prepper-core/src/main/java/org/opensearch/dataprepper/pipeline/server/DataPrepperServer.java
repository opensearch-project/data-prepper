/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
@Named
public class DataPrepperServer {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServer.class);
    private final HttpServerProvider serverProvider;
    private final ListPipelinesHandler listPipelinesHandler;
    private final GetTransformedPipelinesBodyHandler getTransformedPipelinesBodyHandler;
    private final ShutdownHandler shutdownHandler;
    private final PrometheusMeterRegistry prometheusMeterRegistry;
    private final Authenticator authenticator;
    private final ExecutorService executorService;
    private HttpServer server;

    @Inject
    public DataPrepperServer(
            final HttpServerProvider serverProvider,
            final ListPipelinesHandler listPipelinesHandler,
            final ShutdownHandler shutdownHandler,
            final GetTransformedPipelinesBodyHandler getTransformedPipelinesBodyHandler,
            @Autowired(required = false) @Nullable final PrometheusMeterRegistry prometheusMeterRegistry,
            @Autowired(required = false) @Nullable final Authenticator authenticator
    ) {
        this.serverProvider = serverProvider;
        this.listPipelinesHandler = listPipelinesHandler;
        this.shutdownHandler = shutdownHandler;
        this.getTransformedPipelinesBodyHandler = getTransformedPipelinesBodyHandler;
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.authenticator = authenticator;
        executorService = Executors.newFixedThreadPool(3);
    }

    /**
     * Start the DataPrepperServer
     */
    public void start() {
        server = createServer();
        server.setExecutor(executorService);
        server.start();
        LOG.info("Data Prepper server running at :{}", server.getAddress().getPort());
    }

    private HttpServer createServer() {
        final HttpServer server = serverProvider.get();

        createContext(server, listPipelinesHandler, authenticator, "/list");
        createContext(server, shutdownHandler, authenticator, "/shutdown");
        createContext(server, getTransformedPipelinesBodyHandler, authenticator, "/getPipelineBody");

        if (prometheusMeterRegistry != null) {
            final PrometheusMetricsHandler prometheusMetricsHandler = new PrometheusMetricsHandler(prometheusMeterRegistry);
            createContext(server, prometheusMetricsHandler, authenticator, "/metrics/prometheus", "/metrics/sys");
        }

        return server;
    }

    private void createContext(
            final HttpServer httpServer,
            final HttpHandler httpHandler,
            @Nullable final Authenticator authenticator,
            final String ... paths
    ) {
        for (final String path : paths) {
            final HttpContext context = httpServer.createContext(path, httpHandler);

            if (authenticator != null) {
                context.setAuthenticator(authenticator);
            }
        }
    }

    /**
     * Stop the DataPrepperServer
     */
    public void stop() {
        server.stop(0);
        executorService.shutdownNow();
        LOG.info("Data Prepper server stopped");
    }
}
