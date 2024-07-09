/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.peerforwarder.HashRing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToDoubleFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.peerforwarder.discovery.PeerListProvider.PEER_ENDPOINTS;

@ExtendWith(MockitoExtension.class)
public class DnsPeerListProviderTest {

    private static final String ENDPOINT_1 = "10.1.1.1";
    private static final String ENDPOINT_2 = "10.1.1.2";
    private static final List<Endpoint> ENDPOINT_LIST = Arrays.asList(
            Endpoint.of(ENDPOINT_1),
            Endpoint.of(ENDPOINT_2)
    );

    @Mock
    private DnsAddressEndpointGroup dnsAddressEndpointGroup;

    @Mock
    private HashRing hashRing;

    @Mock
    private PluginMetrics pluginMetrics;

    private CompletableFuture completableFuture;

    private DnsPeerListProvider dnsPeerListProvider;

    @BeforeEach
    public void setup() {
        completableFuture = CompletableFuture.completedFuture(null);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);

        dnsPeerListProvider = new DnsPeerListProvider(dnsAddressEndpointGroup, pluginMetrics);
    }

    @Test
    public void testDefaultListProviderWithNullHostname() {
        assertThrows(NullPointerException.class, () -> new DnsPeerListProvider(null, pluginMetrics));
    }

    @Test
    public void testConstructWithInterruptedException() throws Exception {
        CompletableFuture mockFuture = mock(CompletableFuture.class);
        when(mockFuture.get()).thenThrow(new InterruptedException());
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(mockFuture);

        assertThrows(RuntimeException.class, () -> new DnsPeerListProvider(dnsAddressEndpointGroup, pluginMetrics));
    }

    @Test
    public void testGetPeerList() {
        when(dnsAddressEndpointGroup.endpoints()).thenReturn(ENDPOINT_LIST);

        List<String> results = dnsPeerListProvider.getPeerList();

        assertEquals(ENDPOINT_LIST.size(), results.size());
        assertTrue(results.contains(ENDPOINT_1));
        assertTrue(results.contains(ENDPOINT_2));
    }

    @Test
    public void testActivePeerCounter_with_list() {
        when(dnsAddressEndpointGroup.endpoints()).thenReturn(ENDPOINT_LIST);

        final ArgumentCaptor<ToDoubleFunction<DnsAddressEndpointGroup>> gaugeFunctionCaptor = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(PEER_ENDPOINTS), eq(dnsAddressEndpointGroup), gaugeFunctionCaptor.capture());

        final ToDoubleFunction<DnsAddressEndpointGroup> gaugeFunction = gaugeFunctionCaptor.getValue();

        assertThat(gaugeFunction.applyAsDouble(dnsAddressEndpointGroup), equalTo(2.0));
    }

    @Test
    public void testActivePeerCounter_with_single() {
        when(dnsAddressEndpointGroup.endpoints()).thenReturn(Collections.singletonList(Endpoint.of(ENDPOINT_1)));

        final ArgumentCaptor<ToDoubleFunction<DnsAddressEndpointGroup>> gaugeFunctionCaptor = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(PEER_ENDPOINTS), eq(dnsAddressEndpointGroup), gaugeFunctionCaptor.capture());

        final ToDoubleFunction<DnsAddressEndpointGroup> gaugeFunction = gaugeFunctionCaptor.getValue();

        assertThat(gaugeFunction.applyAsDouble(dnsAddressEndpointGroup), equalTo(1.0));
    }

    @Test
    public void testAddListener() {
        dnsPeerListProvider.addListener(hashRing);

        verify(dnsAddressEndpointGroup).addListener(hashRing);
    }

    @Test
    public void testRemoveListener() {
        dnsPeerListProvider.removeListener(hashRing);

        verify(dnsAddressEndpointGroup).removeListener(hashRing);
    }
}
