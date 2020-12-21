package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import java.io.IOException;
import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
public class DataPrepperServer {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServer.class);
    private final HttpServer server;
    private final int port;

    public DataPrepperServer(final DataPrepper dataPrepper) {
        try {
            this.port = DataPrepper.getConfiguration().getServerPort();
            server = HttpServer.create(
                    new InetSocketAddress(this.port),
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
        LOG.info("Data Prepper server running at :{}", port);

    }

    /**
     * Stop the DataPrepperServer
     */
    public void stop() {
        server.stop(0);
        LOG.info("Data Prepper server stopped");
    }
}
