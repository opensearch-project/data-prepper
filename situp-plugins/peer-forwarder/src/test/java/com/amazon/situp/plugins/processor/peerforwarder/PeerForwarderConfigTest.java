package com.amazon.situp.plugins.processor.peerforwarder;

import com.amazon.situp.model.configuration.PluginSetting;
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
        settings.put(PeerForwarderConfig.PEER_IPS, testPeerIps);
        settings.put(PeerForwarderConfig.MAX_NUM_SPANS_PER_REQUEST, testNumSpansPerRequest);
        settings.put(PeerForwarderConfig.TIME_OUT, testTimeout);

        final PeerForwarderConfig peerForwarderConfig = PeerForwarderConfig.buildConfig(
                new PluginSetting("peer_forwarder", settings));

        Assert.assertEquals(testPeerIps, peerForwarderConfig.getPeerIps());
        Assert.assertEquals(testNumSpansPerRequest, peerForwarderConfig.getMaxNumSpansPerRequest());
        Assert.assertEquals(testTimeout, peerForwarderConfig.getTimeOut());
    }
}