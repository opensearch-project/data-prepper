/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.discovery;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.parser.model.ServiceDiscoveryConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;

import java.util.Objects;
import java.util.function.BiFunction;

public enum DiscoveryMode {
    STATIC(StaticPeerListProvider::createPeerListProvider),
    DNS(DnsPeerListProvider::createPeerListProvider),
    AWS_CLOUD_MAP(AwsCloudMapPeerListProvider::createPeerListProvider),
    LOCAL_NODE(LocalPeerListProvider::createPeerListProvider);

    private final BiFunction<ServiceDiscoveryConfiguration, PluginMetrics, PeerListProvider> creationFunction;

    DiscoveryMode(final BiFunction<ServiceDiscoveryConfiguration, PluginMetrics, PeerListProvider> creationFunction) {
        Objects.requireNonNull(creationFunction);

        this.creationFunction = creationFunction;
    }

    /**
     * Creates a new {@link PeerListProvider} for this discovery mode.
     *
     * @param serviceDiscoveryConfiguration The peer forwarder configuration
     * @param pluginMetrics The plugin metrics
     * @return The new {@link PeerListProvider} for this discovery mode
     */
    public PeerListProvider create(final ServiceDiscoveryConfiguration serviceDiscoveryConfiguration, final PluginMetrics pluginMetrics) {
        return creationFunction.apply(serviceDiscoveryConfiguration, pluginMetrics);
    }
}
