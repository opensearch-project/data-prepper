package com.amazon.dataprepper.plugins.processor.peerforwarder;

import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class HashRingTest {
    public static final String TEST_TRACE_ID_1 = "10";
    public static final String TEST_TRACE_ID_2 = "20";

    @Test
    public void testGetServerIpSingleVirtualNode() {
        final List<String> testServerIps = Arrays.asList("20", "30", "10");
        final HashRing hashRing = new HashRing(testServerIps, 1);
        Assert.assertEquals(testServerIps, hashRing.getServerIps());
        // Check indeed alternating serverIps for different inputs
        final String serverIp1 = hashRing.getServerIp(TEST_TRACE_ID_1).orElse("");
        final String serverIp2 = hashRing.getServerIp(TEST_TRACE_ID_2).orElse("");
        Assert.assertNotEquals(serverIp1, serverIp2);
    }

    @Test
    public void testGetServerIpMultipleVirtualNode() {
        final List<String> testServerIps = Collections.singletonList("127.0.0.1");
        final HashRing hashRing = new HashRing(testServerIps, 3);
        Assert.assertEquals(testServerIps, hashRing.getServerIps());
        final TreeMap<Long, String> virtualNodes = Whitebox.getInternalState(hashRing, "virtualNodes");
        Assert.assertEquals(3, virtualNodes.size());
        final String serverIp = hashRing.getServerIp(TEST_TRACE_ID_1).orElse("");
        Assert.assertEquals("127.0.0.1", serverIp);
    }
}
