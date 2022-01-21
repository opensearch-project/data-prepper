/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;



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
            final HttpServer server
    ) {
        this.server = server;
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
