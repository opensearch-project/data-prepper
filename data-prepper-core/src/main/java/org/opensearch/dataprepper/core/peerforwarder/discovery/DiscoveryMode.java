/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.Objects;
import java.util.function.BiFunction;

public enum DiscoveryMode {
    STATIC(StaticPeerListProvider::createPeerListProvider),
    DNS(DnsPeerListProvider::createPeerListProvider),
    AWS_CLOUD_MAP(AwsCloudMapPeerListProvider::createPeerListProvider),
    LOCAL_NODE(LocalPeerListProvider::createPeerListProvider);

    private final BiFunction<PeerForwarderConfiguration, PluginMetrics, PeerListProvider> creationFunction;

    DiscoveryMode(final BiFunction<PeerForwarderConfiguration, PluginMetrics, PeerListProvider> creationFunction) {
        Objects.requireNonNull(creationFunction);

        this.creationFunction = creationFunction;
    }

    /**
     * Creates a new {@link PeerListProvider} for this discovery mode.
     *
     * @param peerForwarderConfiguration The peer forwarder configuration
     * @param pluginMetrics The plugin metrics
     * @return The new {@link PeerListProvider} for this discovery mode
     */
    public PeerListProvider create(final PeerForwarderConfiguration peerForwarderConfiguration, final PluginMetrics pluginMetrics) {
        return creationFunction.apply(peerForwarderConfiguration, pluginMetrics);
    }
}
