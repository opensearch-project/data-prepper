/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Class to handle Peer Forwarder server
 *
 * @since 2.0
 */
public class PeerForwarderServer {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderServer.class);

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final Server sever;

    public PeerForwarderServer(final PeerForwarderConfiguration peerForwarderConfiguration, final Server server) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.sever = server;
    }

    /**
     * Start the PeerForwarderServer
     */
    public void start() {
        try {
            sever.start().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            } else {
                throw new RuntimeException(ex);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        LOG.info("Peer Forwarder server started on port: {}", peerForwarderConfiguration.getServerPort());
    }

    /**
     * Stop the PeerForwarderServer
     */
    public void stop() {
        if (sever != null) {
            try {
                sever.stop().get();
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                } else {
                    throw new RuntimeException(ex);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        LOG.info("Peer Forwarder Server stopped.");
    }

}