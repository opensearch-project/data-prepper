package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DnsPeerListProvider implements PeerListProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DnsPeerListProvider.class);

    private final DnsAddressEndpointGroup endpointGroup;

    public DnsPeerListProvider(final DnsAddressEndpointGroup endpointGroup, final PluginMetrics pluginMetrics) {
        Objects.requireNonNull(endpointGroup);

        this.endpointGroup = endpointGroup;

        try {
            endpointGroup.whenReady().get();
            LOG.info("Found endpoints: {}", String.join(",", endpointGroup.endpoints().toString()));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Caught exception while querying DNS", e);
        }

        pluginMetrics.gauge(PEER_ENDPOINTS, endpointGroup, group -> group.endpoints().size());
    }

    @Override
    public List<String> getPeerList() {
        return endpointGroup.endpoints()
                .stream()
                .map(endpoint -> endpoint.ipAddr())
                .collect(Collectors.toList());
    }

    @Override
    public void addListener(final Consumer<? super List<Endpoint>> listener) {
        endpointGroup.addListener(listener);
    }

    @Override
    public void removeListener(final Consumer<?> listener) {
        endpointGroup.removeListener(listener);
    }
}
