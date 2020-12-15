package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import java.io.IOException;
import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;

/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
public class DataPrepperServer {

    private final HttpServer server;
    private final DataPrepper dataPrepper;

    public DataPrepperServer(final int port, final DataPrepper dataPrepper) {
        this.dataPrepper = dataPrepper;
        try {
            server = HttpServer.create(
                    new InetSocketAddress(port),
                    0
            );
            server.createContext("/metrics/prometheus", new PrometheusMetricsHandler());
            server.createContext("/list", new ListPipelinesHandler(dataPrepper));
            server.createContext("/shutdown", new ShutdownHandler(dataPrepper));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create server", e);
        }
    }

    /**
     * Start the DataPrepperServer
     */
    public void start() {
        server.start();
    }

    /**
     * Stop the DataPrepperServer
     */
    public void stop() {
        server.stop(0);
    }
}
