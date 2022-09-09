/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;

import java.util.Set;

public class PeerForwarderProvider {

    private final PeerForwarderClientFactory peerForwarderClientFactory;
    private final PeerForwarderClient peerForwarderClient;
    private HashRing hashRing;

    PeerForwarderProvider(final PeerForwarderClientFactory peerForwarderClientFactory, final PeerForwarderClient peerForwarderClient) {
        this.peerForwarderClientFactory = peerForwarderClientFactory;
        this.peerForwarderClient = peerForwarderClient;
    }

    public PeerForwarder register(final String pipelineName, final String pluginId, final Set<String> identificationKeys) {

        // TODO: Data Prepper 2.0 will only support a single peer-forwarder per pipeline/plugin type. Check this condition.

        if(hashRing == null) {
            hashRing = peerForwarderClientFactory.createHashRing();
        }

        // TODO: Support a local-only PeerForwarder when no peers are configured.
        return new RemotePeerForwarder(peerForwarderClient, hashRing, pipelineName, pluginId, identificationKeys);
    }
}
