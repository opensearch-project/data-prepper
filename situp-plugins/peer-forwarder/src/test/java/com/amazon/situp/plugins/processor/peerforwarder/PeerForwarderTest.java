package com.amazon.situp.plugins.processor.peerforwarder;

import com.amazon.situp.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class PeerForwarderTest {

    @Test
    public void testEmptyIps() {
        final PeerForwarder testPeerForwarder = generatePeerForwarder(Collections.emptyList());
    }

    private PeerForwarder generatePeerForwarder(final List<String> peerIps) {
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.PEER_IPS, peerIps);
        settings.put(PeerForwarderConfig.MAX_NUM_SPANS_PER_REQUEST, 2);
        settings.put(PeerForwarderConfig.TIME_OUT, 300);
        return new PeerForwarder(new PluginSetting("peer_forwarder", settings));
    }
}