package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;

import java.util.List;
import java.util.Objects;

import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.DISCOVERY_MODE;
import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.HOSTNAME_FOR_DNS_LOOKUP;
import static com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig.STATIC_ENDPOINTS;

public class PeerListProviderFactory {
    // TODO: Make these configurable?
    private static final int MIN_TTL = 10;
    private static final int MAX_TTL = 20;

    public PeerListProvider createProvider(final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginSetting);

        final String discoveryModeString = pluginSetting.getStringOrDefault(DISCOVERY_MODE, null);
        Objects.requireNonNull(discoveryModeString, String.format("Missing '%s' configuration value", DISCOVERY_MODE));

        final DiscoveryMode discoveryMode = DiscoveryMode.valueOf(discoveryModeString.toUpperCase());

        switch (discoveryMode) {
            case DNS:
                final String hostname = pluginSetting.getStringOrDefault(HOSTNAME_FOR_DNS_LOOKUP, null);
                Objects.requireNonNull(hostname, String.format("Missing '%s' configuration value", HOSTNAME_FOR_DNS_LOOKUP));

                final DnsAddressEndpointGroup endpointGroup = DnsAddressEndpointGroup.builder(hostname)
                        .ttl(MIN_TTL, MAX_TTL)
                        .build();

                return new DnsPeerListProvider(endpointGroup);
            case STATIC:
                final List<String> endpoints = (List<String>) pluginSetting.getAttributeOrDefault(STATIC_ENDPOINTS, null);
                Objects.requireNonNull(endpoints, String.format("Missing '%s' configuration value", STATIC_ENDPOINTS));

                return new StaticPeerListProvider(endpoints);
            default:
                throw new UnsupportedOperationException(String.format("Unsupported discovery mode: %s", discoveryMode));
        }
    }
}
