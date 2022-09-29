/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import io.micrometer.core.instrument.Measurement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.peerforwarder.HashRing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DnsPeerListProviderTest {

    private static final String ENDPOINT_1 = "10.1.1.1";
    private static final String ENDPOINT_2 = "10.1.1.2";
    private static final List<Endpoint> ENDPOINT_LIST = Arrays.asList(
            Endpoint.of(ENDPOINT_1),
            Endpoint.of(ENDPOINT_2)
    );
    private static final String COMPONENT_SCOPE = "testComponentScope";
    private static final String COMPONENT_ID = "testComponentId";

    @Mock
    private DnsAddressEndpointGroup dnsAddressEndpointGroup;

    @Mock
    private HashRing hashRing;

    private PluginMetrics pluginMetrics;

    private CompletableFuture completableFuture;

    private DnsPeerListProvider dnsPeerListProvider;

    @Before
    public void setup() {
        MetricsTestUtil.initMetrics();
        completableFuture = CompletableFuture.completedFuture(null);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);

        pluginMetrics = PluginMetrics.fromNames(COMPONENT_ID, COMPONENT_SCOPE);
        dnsPeerListProvider = new DnsPeerListProvider(dnsAddressEndpointGroup, pluginMetrics);
    }

    @Test(expected = NullPointerException.class)
    public void testDefaultListProviderWithNullHostname() {
        new DnsPeerListProvider(null, pluginMetrics);
    }

    @Test(expected = RuntimeException.class)
    public void testConstructWithInterruptedException() throws Exception {
        CompletableFuture mockFuture = mock(CompletableFuture.class);
        when(mockFuture.get()).thenThrow(new InterruptedException());
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(mockFuture);

        new DnsPeerListProvider(dnsAddressEndpointGroup, pluginMetrics);
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
    public void testActivePeerCounter() {
        when(dnsAddressEndpointGroup.endpoints()).thenReturn(ENDPOINT_LIST);

        final List<Measurement> endpointsMeasures = MetricsTestUtil.getMeasurementList(new StringJoiner(MetricNames.DELIMITER).add(COMPONENT_SCOPE).add(COMPONENT_ID)
                .add(PeerListProvider.PEER_ENDPOINTS).toString());
        assertEquals(1, endpointsMeasures.size());
        final Measurement endpointsMeasure = endpointsMeasures.get(0);
        assertEquals(2.0, endpointsMeasure.getValue(), 0);

        when(dnsAddressEndpointGroup.endpoints()).thenReturn(Collections.singletonList(Endpoint.of(ENDPOINT_1)));
        assertEquals(1.0, endpointsMeasure.getValue(), 0);
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
