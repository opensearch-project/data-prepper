package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import java.io.IOException;
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
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        } catch (Exception e) {
            LOG.error("Caught exception shutting down data prepper", e);
            exchange.sendResponseHeaders(500, 0);
            exchange.getResponseBody().close();
        }
    }
}
