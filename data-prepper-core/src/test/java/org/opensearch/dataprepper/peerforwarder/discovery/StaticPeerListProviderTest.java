/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.discovery;

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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(MockitoJUnitRunner.class)
public class StaticPeerListProviderTest {

    private static final String ENDPOINT_1 = "10.10.0.1";
    private static final String ENDPOINT_2 = "10.10.0.2";
    private static final List<String> ENDPOINT_LIST = Arrays.asList(ENDPOINT_1, ENDPOINT_2);
    private static final String COMPONENT_SCOPE = "testComponentScope";
    private static final String COMPONENT_ID = "testComponentId";

    @Mock
    private HashRing hashRing;

    private PluginMetrics pluginMetrics;

    private StaticPeerListProvider staticPeerListProvider;

    @Before
    public void setup() {
        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames(COMPONENT_ID, COMPONENT_SCOPE);
        staticPeerListProvider = new StaticPeerListProvider(ENDPOINT_LIST, pluginMetrics);
    }

    @Test(expected = RuntimeException.class)
    public void testListProviderWithEmptyList() {
        new StaticPeerListProvider(Collections.emptyList(), pluginMetrics);
    }

    @Test(expected = RuntimeException.class)
    public void testListProviderWithNullList() {
        new StaticPeerListProvider(null, pluginMetrics);
    }

    @Test
    public void testListProviderWithNonEmptyList() {
        assertEquals(ENDPOINT_LIST.size(), staticPeerListProvider.getPeerList().size());
        assertEquals(ENDPOINT_LIST, staticPeerListProvider.getPeerList());
    }

    @Test
    public void testActivePeerCounter() {
        final List<Measurement> endpointsMeasures = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(COMPONENT_SCOPE).add(COMPONENT_ID).add(PeerListProvider.PEER_ENDPOINTS).toString());
        assertEquals(1, endpointsMeasures.size());
        final Measurement endpointsMeasure = endpointsMeasures.get(0);
        assertEquals(2.0, endpointsMeasure.getValue(), 0);
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
