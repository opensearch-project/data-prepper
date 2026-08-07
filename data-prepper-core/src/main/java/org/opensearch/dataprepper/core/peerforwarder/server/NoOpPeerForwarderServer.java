/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
