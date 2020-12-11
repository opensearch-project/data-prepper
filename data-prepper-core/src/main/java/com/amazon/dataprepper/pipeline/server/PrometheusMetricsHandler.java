package com.amazon.dataprepper.pipeline.server;

import java.io.IOException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpHandler to handle requests for Prometheus metrics
 */
public class PrometheusMetricsHandler implements HttpHandler {

    private PrometheusMeterRegistry prometheusMeterRegistry;
    private final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsHandler.class);

    public PrometheusMetricsHandler() {
        prometheusMeterRegistry = (PrometheusMeterRegistry) Metrics.globalRegistry.getRegistries().iterator().next();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            byte response[] = prometheusMeterRegistry.scrape().getBytes("UTF-8");
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.getResponseBody().close();
        } catch (Exception e) {
            LOG.error("Encountered exception scarping prometheus meter registry", e);
            exchange.sendResponseHeaders(500, 0);
            exchange.getResponseBody().close();
        }
    }
}
