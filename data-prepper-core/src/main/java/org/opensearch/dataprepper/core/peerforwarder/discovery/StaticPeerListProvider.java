/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import com.google.common.base.Preconditions;
import com.linecorp.armeria.client.Endpoint;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class StaticPeerListProvider implements PeerListProvider {

    private static final Logger LOG = LoggerFactory.getLogger(StaticPeerListProvider.class);

    public static final String LOCAL_ENDPOINT = "127.0.0.1";

    private final List<String> endpoints;

    public StaticPeerListProvider(final List<String> dataPrepperEndpoints, final PluginMetrics pluginMetrics) {
        if (dataPrepperEndpoints != null && !dataPrepperEndpoints.isEmpty()) {
            endpoints = Collections.unmodifiableList(dataPrepperEndpoints);
        } else {
            throw new RuntimeException("Peer endpoints list cannot be empty");
        }
        LOG.info("Found endpoints: {}", endpoints);

        pluginMetrics.gauge(PEER_ENDPOINTS, endpoints, List::size);
    }

    static StaticPeerListProvider createPeerListProvider(final PeerForwarderConfiguration peerForwarderConfiguration, final PluginMetrics pluginMetrics) {
        final List<String> endpoints = peerForwarderConfiguration.getStaticEndpoints();
        Objects.requireNonNull(endpoints, "Missing static_endpoints configuration value");
        final List<String> invalidEndpoints = endpoints.stream().filter(endpoint -> !DiscoveryUtils.validateEndpoint(endpoint)).collect(Collectors.toList());
        Preconditions.checkState(invalidEndpoints.isEmpty(), "Including invalid endpoints: %s", invalidEndpoints);

        return new StaticPeerListProvider(endpoints, pluginMetrics);
    }

    @Override
    public List<String> getPeerList() {
        return endpoints;
    }

    @Override
    public void addListener(final Consumer<? super List<Endpoint>> listener) {
        // Do nothing - static peer list isn't expected to change
    }

    @Override
    public void removeListener(final Consumer<?> listener) {
        // Do nothing - static peer list isn't expected to change
    }
}
