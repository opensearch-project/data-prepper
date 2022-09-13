/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.discovery;

import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;

import java.util.Objects;
import java.util.function.Function;

public enum DiscoveryMode {
    STATIC(StaticPeerListProvider::createPeerListProvider),
    DNS(DnsPeerListProvider::createPeerListProvider),
    AWS_CLOUD_MAP(AwsCloudMapPeerListProvider::createPeerListProvider),
    LOCAL_NODE(LocalPeerListProvider::createPeerListProvider);

    private final Function<PeerForwarderConfiguration, PeerListProvider> creationFunction;

    DiscoveryMode(final Function<PeerForwarderConfiguration, PeerListProvider> creationFunction) {
        Objects.requireNonNull(creationFunction);

        this.creationFunction = creationFunction;
    }

    /**
     * Creates a new {@link PeerListProvider} for this discovery mode.
     *
     * @param peerForwarderConfiguration The plugin settings
     * @return The new {@link PeerListProvider} for this discovery mode
     */
    public PeerListProvider create(PeerForwarderConfiguration peerForwarderConfiguration) {
        return creationFunction.apply(peerForwarderConfiguration);
    }
}
