package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.PeerForwarderConfig;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;

import java.util.List;
import java.util.Objects;

public class PeerListProviderFactory {
    // TODO: Make these configurable?
    private static final int MIN_TTL = 10;
    private static final int MAX_TTL = 20;

    public PeerListProvider createProvider(final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginSetting);

        final String discoveryModeString = pluginSetting.getStringOrDefault(PeerForwarderConfig.DISCOVERY_MODE, null);
        Objects.requireNonNull(discoveryModeString, String.format("Missing '%s' configuration value", PeerForwarderConfig.DISCOVERY_MODE));

        final DiscoveryMode discoveryMode = DiscoveryMode.valueOf(discoveryModeString.toUpperCase());

        final PluginMetrics pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);

        switch (discoveryMode) {
            case DNS:
                final String domainName = pluginSetting.getStringOrDefault(PeerForwarderConfig.DOMAIN_NAME, null);
                Objects.requireNonNull(domainName, String.format("Missing '%s' configuration value",PeerForwarderConfig. DOMAIN_NAME));

                final DnsAddressEndpointGroup endpointGroup = DnsAddressEndpointGroup.builder(domainName)
                        .ttl(MIN_TTL, MAX_TTL)
                        .build();

                return new DnsPeerListProvider(endpointGroup, pluginMetrics);
            case STATIC:
                final List<String> endpoints = (List<String>) pluginSetting.getAttributeOrDefault(PeerForwarderConfig.STATIC_ENDPOINTS, null);
                Objects.requireNonNull(endpoints, String.format("Missing '%s' configuration value", PeerForwarderConfig.STATIC_ENDPOINTS));

                return new StaticPeerListProvider(endpoints, pluginMetrics);
            default:
                throw new UnsupportedOperationException(String.format("Unsupported discovery mode: %s", discoveryMode));
        }
    }
}
