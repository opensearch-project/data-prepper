package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class StaticPeerListProvider implements PeerListProvider {
    private static final Logger LOG = LoggerFactory.getLogger(StaticPeerListProvider.class);

    public static final String LOCAL_ENDPOINT = "127.0.0.1";
    public static final List<String> DEFAULT_LIST = Collections.singletonList(LOCAL_ENDPOINT);

    private final List<String> endpoints;

    public StaticPeerListProvider(final List<String> dataPrepperEndpoints) {
        if (dataPrepperEndpoints != null && dataPrepperEndpoints.size() > 0) {
            endpoints = Collections.unmodifiableList(dataPrepperEndpoints);
        } else {
            LOG.warn("No endpoints provided, defaulting to localhost only.");
            endpoints = DEFAULT_LIST;
        }

        LOG.info("Found endpoints: {}", String.join(",", endpoints));
    }

    public StaticPeerListProvider() {
        this(Collections.emptyList());
    }

    @Override
    public List<String> getPeerList() {
        return endpoints;
    }
}
