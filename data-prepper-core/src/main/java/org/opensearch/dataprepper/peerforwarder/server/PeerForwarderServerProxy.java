/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;

public class PeerForwarderServerProxy implements PeerForwarderServer {
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final Server server;

    private PeerForwarderServer peerForwarderServer;

    public PeerForwarderServerProxy(final PeerForwarderConfiguration peerForwarderConfiguration, final Server server) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.server = server;
    }

    @Override
    public void start() {
        // update this conditional based on PeerForwarderProvider
        if (true) {
            peerForwarderServer = new RemotePeerForwarderServer(peerForwarderConfiguration, server);
        }
        else {
            peerForwarderServer = new NoOpPeerForwarderServer();
        }
        peerForwarderServer.start();
    }

    @Override
    public void stop() {
        peerForwarderServer.stop();
    }
}
