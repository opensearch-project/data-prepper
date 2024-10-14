/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Class to handle remote Peer Forwarder server if peers are configured
 *
 * @since 2.0
 */
public class RemotePeerForwarderServer implements PeerForwarderServer {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePeerForwarderServer.class);

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final Server sever;

    public RemotePeerForwarderServer(final PeerForwarderConfiguration peerForwarderConfiguration, final Server server) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.sever = server;
    }

    @Override
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

    @Override
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