package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.DiscoveryMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class PeerForwarderConfigTest {

    @Test
    public void testBuildConfig() {
        final List<String> testPeerIps = Arrays.asList("172.0.0.1", "172.0.0.2");
        final int testNumSpansPerRequest = 2;
        final int testTimeout = 300;
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());
        settings.put(PeerForwarderConfig.STATIC_ENDPOINTS, testPeerIps);
        settings.put(PeerForwarderConfig.MAX_NUM_SPANS_PER_REQUEST, testNumSpansPerRequest);
        settings.put(PeerForwarderConfig.TIME_OUT, testTimeout);

        final PeerForwarderConfig peerForwarderConfig = PeerForwarderConfig.buildConfig(
                new PluginSetting("peer_forwarder", settings));

        Assert.assertEquals(testNumSpansPerRequest, peerForwarderConfig.getMaxNumSpansPerRequest());
        Assert.assertEquals(testTimeout, peerForwarderConfig.getTimeOut());
    }
}