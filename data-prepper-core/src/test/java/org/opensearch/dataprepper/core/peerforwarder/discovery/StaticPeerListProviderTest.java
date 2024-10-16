/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.discovery.StaticPeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.core.peerforwarder.HashRing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.ToDoubleFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider.PEER_ENDPOINTS;

@ExtendWith(MockitoExtension.class)
public class StaticPeerListProviderTest {

    private static final String ENDPOINT_1 = "10.10.0.1";
    private static final String ENDPOINT_2 = "10.10.0.2";
    private static final List<String> ENDPOINT_LIST = Arrays.asList(ENDPOINT_1, ENDPOINT_2);

    @Mock
    private HashRing hashRing;

    @Mock
    private PluginMetrics pluginMetrics;

    private StaticPeerListProvider staticPeerListProvider;

    @BeforeEach
    public void setup() {
        staticPeerListProvider = new StaticPeerListProvider(ENDPOINT_LIST, pluginMetrics);
    }

    @Test
    public void testListProviderWithEmptyList() {
        assertThrows(RuntimeException.class, () -> new StaticPeerListProvider(Collections.emptyList(), pluginMetrics));
    }

    @Test
    public void testListProviderWithNullList() {
        assertThrows(RuntimeException.class, () -> new StaticPeerListProvider(null, pluginMetrics));
    }

    @Test
    public void testListProviderWithNonEmptyList() {
        assertEquals(ENDPOINT_LIST.size(), staticPeerListProvider.getPeerList().size());
        assertEquals(ENDPOINT_LIST, staticPeerListProvider.getPeerList());
    }

    @Test
    public void testActivePeerCounter() {
        final ArgumentCaptor<ToDoubleFunction<List<String>>> gaugeFunctionCaptor = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(PEER_ENDPOINTS), any(List.class), gaugeFunctionCaptor.capture());

        final ToDoubleFunction<List<String>> gaugeFunction = gaugeFunctionCaptor.getValue();

        assertThat(gaugeFunction.applyAsDouble(ENDPOINT_LIST), equalTo(2.0));
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
