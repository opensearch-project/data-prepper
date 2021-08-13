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
import com.amazon.dataprepper.plugins.prepper.peerforwarder.PeerForwarderConfig;

import java.util.Objects;

public class PeerListProviderFactory {

    public PeerListProvider createProvider(final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginSetting);

        final String discoveryModeString = pluginSetting.getStringOrDefault(PeerForwarderConfig.DISCOVERY_MODE, null);
        Objects.requireNonNull(discoveryModeString, String.format("Missing '%s' configuration value", PeerForwarderConfig.DISCOVERY_MODE));

        final DiscoveryMode discoveryMode = DiscoveryMode.valueOf(discoveryModeString.toUpperCase());

        final PluginMetrics pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);

        return discoveryMode.create(pluginSetting, pluginMetrics);
    }

}
