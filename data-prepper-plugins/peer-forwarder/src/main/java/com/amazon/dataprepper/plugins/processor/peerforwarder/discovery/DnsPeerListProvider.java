package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DnsPeerListProvider implements PeerListProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DnsPeerListProvider.class);

    private final DnsAddressEndpointGroup endpointGroup;

    public DnsPeerListProvider(final DnsAddressEndpointGroup endpointGroup) {
        Objects.requireNonNull(endpointGroup);

        this.endpointGroup = endpointGroup;

        try {
            endpointGroup.whenReady().get();
            LOG.info("Found endpoints: {}", String.join(",", endpointGroup.endpoints().toString()));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Caught exception while querying DNS", e);
        }
    }

    @Override
    public List<String> getPeerList() {
        return endpointGroup.endpoints()
                .stream()
                .map(endpoint -> endpoint.ipAddr())
                .collect(Collectors.toList());
    }
}
