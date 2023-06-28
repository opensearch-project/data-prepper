/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Runs an in-memory, "embedded" instance of a ZooKeeper server.
 *
 * The ZooKeeper server instance is automatically started when you create a new instance of this class.
 */
public class EmbeddedZooKeeperServer {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedZooKeeperServer.class);

    private final TestingServer server;

    public EmbeddedZooKeeperServer() throws Exception {
        log.debug("Starting embedded ZooKeeper server...");
        this.server = new TestingServer();
        log.debug("Embedded ZooKeeper server at {} uses the temp directory at {}",
                server.getConnectString(), server.getTempDirectory());
    }

    public void stop() throws IOException {
        log.debug("Shutting down embedded ZooKeeper server at {} ...", server.getConnectString());
        server.close();
        log.debug("Shutdown of embedded ZooKeeper server at {} completed", server.getConnectString());
    }

    public String connectString() {
        return server.getConnectString();
    }

    public String hostname() {
        return connectString().substring(0, connectString().lastIndexOf(':'));
    }

}
