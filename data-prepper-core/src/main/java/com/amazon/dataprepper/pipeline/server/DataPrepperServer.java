/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;



/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
@Named
public class DataPrepperServer {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServer.class);
    private final HttpServer server;

    @Inject
    public DataPrepperServer(
            final Optional<PrometheusMeterRegistry> prometheusMeterRegistry,
            final Optional<Authenticator> optionalAuthenticator,
            final ListPipelinesHandler listPipelinesHandler,
            final ShutdownHandler shutdownHandler,
            final HttpServer server
    ) {
        this.server = server;

        List<HttpContext> contextList = new ArrayList<>(4);
        prometheusMeterRegistry.ifPresent(metricRegistry -> {
            contextList.add(server.createContext("/metrics/prometheus", new PrometheusMetricsHandler(metricRegistry)));
            contextList.add(server.createContext("/metrics/sys", new PrometheusMetricsHandler(metricRegistry)));
        });

        contextList.add(server.createContext("/list", listPipelinesHandler));
        contextList.add(server.createContext("/shutdown", shutdownHandler));

        optionalAuthenticator.ifPresent(
                authenticator -> contextList.forEach(
                        context -> context.setAuthenticator(authenticator)));
    }

    private Optional<MeterRegistry> getPrometheusMeterRegistryFromRegistries(final Set<MeterRegistry> meterRegistries) {
        return meterRegistries.stream().filter(meterRegistry ->
                meterRegistry instanceof PrometheusMeterRegistry).findFirst();
    }

    /**
     * Start the DataPrepperServer
     */
    public void start() {
        server.start();
        LOG.info("Data Prepper server running at :{}", server.getAddress().getPort());

    }

    /**
     * Stop the DataPrepperServer
     */
    public void stop() {
        server.stop(0);
        LOG.info("Data Prepper server stopped");
    }
}
