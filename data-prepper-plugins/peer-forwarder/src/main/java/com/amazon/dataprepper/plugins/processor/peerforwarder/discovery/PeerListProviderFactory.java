package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.HOSTNAME_FOR_DNS_LOOKUP;
import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.STATIC_ENDPOINTS;
import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.DISCOVERY_MODE;

public class PeerListProviderFactory {
    // TODO: Make these configurable?
    private static final int MIN_TTL = 10;
    private static final int MAX_TTL = 20;

    public PeerListProvider createProvider(final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginSetting);

        final String discoveryModeString = pluginSetting.getStringOrDefault(DISCOVERY_MODE, DiscoveryMode.STATIC.toString()).toUpperCase();
        final DiscoveryMode discoveryMode = DiscoveryMode.valueOf(discoveryModeString);

        switch (discoveryMode) {
            case DNS:
                final String hostname = pluginSetting.getStringOrDefault(HOSTNAME_FOR_DNS_LOOKUP, null);
                Objects.requireNonNull(hostname);

                final DnsAddressEndpointGroup endpointGroup = DnsAddressEndpointGroup.builder(hostname)
                        .ttl(MIN_TTL, MAX_TTL)
                        .build();

                return new DnsPeerListProvider(endpointGroup);
            case STATIC:
                final List<String> endpoints = (List<String>) pluginSetting.getAttributeOrDefault(STATIC_ENDPOINTS, Collections.emptyList());
                return new StaticPeerListProvider(endpoints);
            default:
                return new StaticPeerListProvider();
        }
    }
}
