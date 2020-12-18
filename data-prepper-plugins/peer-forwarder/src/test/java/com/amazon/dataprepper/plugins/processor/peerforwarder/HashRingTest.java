package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.PeerListProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HashRingTest {
    private static final List<String> SERVER_IPS = Arrays.asList(
            "10.10.0.1",
            "10.10.0.2",
            "10.10.0.3",
            "10.10.0.4",
            "10.10.0.5");
    private static final String TRACE_ID_1 = "TRACE_1";
    private static final String TRACE_ID_2 = "TRACE_2";
    private static final int SINGLE_VIRTUAL_NODE_COUNT = 1;
    private static final int MULTIPLE_VIRTUAL_NODE_COUNT = 100;

    @Mock
    private PeerListProvider peerListProvider;

    private HashRing sut;

    @Before
    public void setup() {
        when(peerListProvider.getPeerList()).thenReturn(SERVER_IPS);
    }

    @Test
    public void testGetServerIpEmptyMap() {
        when(peerListProvider.getPeerList()).thenReturn(Collections.emptyList());
        sut = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        Optional<String> result = sut.getServerIp(TRACE_ID_1);

        assertFalse(result.isPresent());
    }

    @Test
    public void testGetServerIpSingleNodeSameTraceIds() {
        sut = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = sut.getServerIp(TRACE_ID_1);
        Optional<String> result2 = sut.getServerIp(TRACE_ID_1);

        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertEquals(result1.get(), result2.get());
    }

    @Test
    public void testGetServerIpSingleNodeDifferentTraceIds() {
        sut = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = sut.getServerIp(TRACE_ID_1);
        Optional<String> result2 = sut.getServerIp(TRACE_ID_2);

        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertNotEquals(result1.get(), result2.get());
    }

    @Test
    public void testGetServerIpMultipleNodesSameTraceIds() {
        sut = new HashRing(peerListProvider, MULTIPLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = sut.getServerIp(TRACE_ID_1);
        Optional<String> result2 = sut.getServerIp(TRACE_ID_1);

        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertEquals(result1.get(), result2.get());
    }

    @Test
    public void testGetServerIpMultipleDifferentTraceIds() {
        sut = new HashRing(peerListProvider, MULTIPLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = sut.getServerIp(TRACE_ID_1);
        Optional<String> result2 = sut.getServerIp(TRACE_ID_2);

        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertNotEquals(result1.get(), result2.get());
    }

    @Test
    public void testSpecialCaseNoKeyInMapGreaterThanHashValue() {
        when(peerListProvider.getPeerList()).thenReturn(Collections.singletonList("serverIp"));

        sut = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        // Trace ID 1 hash is less than the hash of "serverIp"
        Optional<String> result1 = sut.getServerIp(TRACE_ID_1);

        // Trace ID 2 hash is greater than the hash of "serverIp"
        Optional<String> result2 = sut.getServerIp(TRACE_ID_2);

        // As there is only one value in the server list, the results should be the same
        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertEquals(result1.get(), result2.get());
    }

    @Test
    public void testEndpointChangeRebuildsMap() {
        sut = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        // First call during construction
        verify(peerListProvider, times(1)).getPeerList();

        sut.accept(Collections.emptyList());

        // Second call during rebuild
        verify(peerListProvider, times(2)).getPeerList();
    }
}
