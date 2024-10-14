/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderProvider;

public class PeerForwarderServerProxy implements PeerForwarderServer {
    private final PeerForwarderHttpServerProvider peerForwarderHttpServerProvider;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerForwarderProvider peerForwarderProvider;

    private PeerForwarderServer peerForwarderServer;

    public PeerForwarderServerProxy(final PeerForwarderHttpServerProvider peerForwarderHttpServerProvider,
                                    final PeerForwarderConfiguration peerForwarderConfiguration,
                                    final PeerForwarderProvider peerForwarderProvider) {
        this.peerForwarderHttpServerProvider = peerForwarderHttpServerProvider;
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerForwarderProvider = peerForwarderProvider;
    }

    @Override
    public void start() {
        if (peerForwarderProvider.isPeerForwardingRequired()) {
            final Server server = peerForwarderHttpServerProvider.get();
            peerForwarderServer = new RemotePeerForwarderServer(peerForwarderConfiguration, server);
        }
        else {
            peerForwarderServer = new NoOpPeerForwarderServer();
        }
        peerForwarderServer.start();
    }

    @Override
    public void stop() {
        if (peerForwarderServer != null) {
            peerForwarderServer.stop();
        }
    }
}
