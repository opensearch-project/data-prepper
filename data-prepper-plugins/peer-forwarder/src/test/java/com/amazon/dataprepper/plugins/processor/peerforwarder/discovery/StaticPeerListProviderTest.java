package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.plugins.processor.peerforwarder.HashRing;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(MockitoJUnitRunner.class)
public class StaticPeerListProviderTest {
    private static final String ENDPOINT_1 = "10.10.0.1";
    private static final String ENDPOINT_2 = "10.10.0.2";
    private static final List<String> ENDPOINT_LIST = Arrays.asList(ENDPOINT_1, ENDPOINT_2);

    @Mock
    private HashRing hashRing;

    private StaticPeerListProvider staticPeerListProvider;

    @Before
    public void setup() {
        staticPeerListProvider = new StaticPeerListProvider(ENDPOINT_LIST);
    }

    @Test(expected = RuntimeException.class)
    public void testListProviderWithEmptyList() {
        new StaticPeerListProvider(Collections.emptyList());
    }

    @Test(expected = RuntimeException.class)
    public void testListProviderWithNullList() {
        new StaticPeerListProvider(null);
    }

    @Test
    public void testListProviderWithNonEmptyList() {
        assertEquals(ENDPOINT_LIST.size(), staticPeerListProvider.getPeerList().size());
        assertEquals(ENDPOINT_LIST, staticPeerListProvider.getPeerList());
    }

    @Test
    public void testAddListener() {
        verifyNoInteractions(hashRing);
        staticPeerListProvider.addListener(hashRing);

    }

    @Test
    public void testRemoveListener() {
        verifyNoInteractions(hashRing);
        staticPeerListProvider.removeListener(hashRing);
    }
}
