/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.HashRing;
import io.micrometer.core.instrument.Measurement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(MockitoJUnitRunner.class)
public class StaticPeerListProviderTest {
    private static final String PLUGIN_NAME = "peer_forwarder";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final PluginSetting PLUGIN_SETTING = new PluginSetting(PLUGIN_NAME, Collections.emptyMap()) {{
        setPipelineName(PIPELINE_NAME);
    }};
    private static final PluginMetrics PLUGIN_METRICS = PluginMetrics.fromPluginSetting(PLUGIN_SETTING);

    private static final String ENDPOINT_1 = "10.10.0.1";
    private static final String ENDPOINT_2 = "10.10.0.2";
    private static final List<String> ENDPOINT_LIST = Arrays.asList(ENDPOINT_1, ENDPOINT_2);

    @Mock
    private HashRing hashRing;

    private StaticPeerListProvider staticPeerListProvider;

    @Before
    public void setup() {
        MetricsTestUtil.initMetrics();
        staticPeerListProvider = new StaticPeerListProvider(ENDPOINT_LIST, PLUGIN_METRICS);
    }

    @Test(expected = RuntimeException.class)
    public void testListProviderWithEmptyList() {
        new StaticPeerListProvider(Collections.emptyList(), PLUGIN_METRICS);
    }

    @Test(expected = RuntimeException.class)
    public void testListProviderWithNullList() {
        new StaticPeerListProvider(null, PLUGIN_METRICS);
    }

    @Test
    public void testListProviderWithNonEmptyList() {
        assertEquals(ENDPOINT_LIST.size(), staticPeerListProvider.getPeerList().size());
        assertEquals(ENDPOINT_LIST, staticPeerListProvider.getPeerList());
    }

    @Test
    public void testActivePeerCounter() {
        final List<Measurement> endpointsMeasures = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME).add(PeerListProvider.PEER_ENDPOINTS).toString());
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
