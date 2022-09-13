/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;

public class PeerForwarderServerProxy implements PeerForwarderServer {
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final Server server;
    private final PeerForwarderProvider peerForwarderProvider;

    private PeerForwarderServer peerForwarderServer;

    public PeerForwarderServerProxy(final PeerForwarderConfiguration peerForwarderConfiguration,
                                    final Server server,
                                    final PeerForwarderProvider peerForwarderProvider) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.server = server;
        this.peerForwarderProvider = peerForwarderProvider;
    }

    @Override
    public void start() {
        if (peerForwarderProvider.isPeerForwardingRequired()) {
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
