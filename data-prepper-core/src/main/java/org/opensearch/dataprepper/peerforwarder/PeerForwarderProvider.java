/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

public class PeerForwarderProvider {
    private final PeerForwarder peerForwarder;

    public PeerForwarderProvider(final PeerForwarder peerForwarder) {
        this.peerForwarder = peerForwarder;
    }

    public PeerForwarder register() {
        // TODO: Refactor the AppConfig and PeerForwarder so that we only create these objects when necessary.
        // TODO: Refactor PeerForward to make it an interface. Support both a configured peer-forward and a no-op local-only. Provide the one appropriate based on user configuration.
        return peerForwarder;
    }
}
