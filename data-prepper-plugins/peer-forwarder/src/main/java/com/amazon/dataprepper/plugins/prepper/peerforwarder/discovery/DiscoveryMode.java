/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.util.Objects;
import java.util.function.BiFunction;

public enum DiscoveryMode {
    STATIC(StaticPeerListProvider::createPeerListProvider),
    DNS(DnsPeerListProvider::createPeerListProvider),
    AWS_CLOUD_MAP(AwsCloudMapPeerListProvider::createPeerListProvider);

    private final BiFunction<PluginSetting, PluginMetrics, PeerListProvider> creationFunction;

    DiscoveryMode(final BiFunction<PluginSetting, PluginMetrics, PeerListProvider> creationFunction) {
        Objects.requireNonNull(creationFunction);

        this.creationFunction = creationFunction;
    }

    /**
     * Creates a new {@link PeerListProvider} for the this discovery mode.
     *
     * @param pluginSetting The plugin settings
     * @param pluginMetrics The plugin metrics
     * @return The new {@link PeerListProvider} for this discovery mode
     */
    PeerListProvider create(PluginSetting pluginSetting, PluginMetrics pluginMetrics) {
        return creationFunction.apply(pluginSetting, pluginMetrics);
    }
}
