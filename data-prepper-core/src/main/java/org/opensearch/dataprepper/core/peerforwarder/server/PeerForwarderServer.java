/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

/**
 * Interface to start and stop Peer Forwarder server
 *
 * @since 2.0
 */
public interface PeerForwarderServer {
    /**
     * Start the PeerForwarderServer
     * @since 2.0
     */
    void start();

    /**
     * Stop the PeerForwarderServer
     * @since 2.0
     */
    void stop();
}
