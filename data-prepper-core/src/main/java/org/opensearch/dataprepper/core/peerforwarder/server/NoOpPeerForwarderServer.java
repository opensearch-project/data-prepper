/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

/**
 * Class to handle NoOp Peer Forwarder server if no peers are configured
 *
 * @since 2.0
 */
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
