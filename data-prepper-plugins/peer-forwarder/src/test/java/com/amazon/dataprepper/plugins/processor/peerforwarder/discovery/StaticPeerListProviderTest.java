package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StaticPeerListProviderTest {
    private static final String ENDPOINT_1 = "10.10.0.1";
    private static final String ENDPOINT_2 = "10.10.0.2";
    private static final List<String> ENDPOINT_LIST = Arrays.asList(ENDPOINT_1, ENDPOINT_2);

    @Test(expected = RuntimeException.class)
    public void testListProviderWithEmptyList() {
        new StaticPeerListProvider(Collections.emptyList());
    }

    @Test
    public void testListProviderWithNonEmptyList() {
        StaticPeerListProvider listProvider = new StaticPeerListProvider(ENDPOINT_LIST);

        assertEquals(ENDPOINT_LIST.size(), listProvider.getPeerList().size());
        assertEquals(ENDPOINT_LIST, listProvider.getPeerList());
    }
}
