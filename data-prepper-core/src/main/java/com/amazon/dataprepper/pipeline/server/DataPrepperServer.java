package com.amazon.dataprepper.pipeline.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.amazon.dataprepper.DataPrepper;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.sun.net.httpserver.HttpServer;

/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
public class DataPrepperServer {

    private final HttpServer server;

    public DataPrepperServer(final int port, final DataPrepper dataPrepper) {
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
     * Stop the data prepper server, with a delay
     * @param delay delay in milliseconds
     */
    public void stop(long delay) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder().setDaemon(false).setNameFormat("server-shutdown-pool-%d").build());
        scheduledExecutorService.schedule(() -> server.stop(0) , delay, TimeUnit.MILLISECONDS);
        scheduledExecutorService.shutdown();
    }
}
