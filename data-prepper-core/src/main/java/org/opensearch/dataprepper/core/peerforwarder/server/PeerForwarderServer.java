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
