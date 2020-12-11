package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.DATA_PREPPER_IPS;
import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.DISCOVERY_MODE;

public class PeerListProviderFactory {
    public PeerListProvider createProvider(final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginSetting);

        final String discoveryModeString = pluginSetting.getStringOrDefault(DISCOVERY_MODE, DiscoveryMode.STATIC.toString()).toUpperCase();
        final DiscoveryMode discoveryMode = DiscoveryMode.valueOf(discoveryModeString);

        switch (discoveryMode) {
            case STATIC:
                final List<String> endpoints = (List<String>) pluginSetting.getAttributeOrDefault(DATA_PREPPER_IPS, Collections.emptyList());
                return new StaticPeerListProvider(endpoints);
            default:
                return new StaticPeerListProvider();
        }
    }
}
