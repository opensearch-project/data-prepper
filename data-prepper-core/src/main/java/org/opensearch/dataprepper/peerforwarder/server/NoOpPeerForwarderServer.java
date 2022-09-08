/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

public class NoOpPeerForwarderServer implements PeerForwarderServer {

    @Override
    public void start() {
        // no need to start the server as no peers are configured
    }

    @Override
    public void stop() {
        // server never starts if no peers are configured
    }
}
