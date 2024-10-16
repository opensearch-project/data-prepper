/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider;
import org.opensearch.dataprepper.core.peerforwarder.discovery.StaticPeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StaticPeerListProviderCreationTest {

    private static final String ENDPOINT = "ENDPOINT";
    private static final String INVALID_ENDPOINT = "INVALID_ENDPOINT_";

    private PeerForwarderConfiguration peerForwarderConfiguration;
    private PluginMetrics pluginMetrics;

    @BeforeEach
    void setup() {
        peerForwarderConfiguration = mock(PeerForwarderConfiguration.class);
        pluginMetrics = mock(PluginMetrics.class);
    }

    @Test
    void testCreateProviderStaticInstanceNoEndpoints() {
        when(peerForwarderConfiguration.getStaticEndpoints()).thenReturn(null);

        assertThrows(NullPointerException.class,
                () -> StaticPeerListProvider.createPeerListProvider(peerForwarderConfiguration, pluginMetrics));
    }

    @Test
    void testCreateProviderStaticInstanceWithEndpoints() {
        when(peerForwarderConfiguration.getStaticEndpoints()).thenReturn(Collections.singletonList(ENDPOINT));

        PeerListProvider result = StaticPeerListProvider.createPeerListProvider(peerForwarderConfiguration, pluginMetrics);

        assertThat(result, instanceOf(StaticPeerListProvider.class));
        assertEquals(1, result.getPeerList().size());
        assertTrue(result.getPeerList().contains(ENDPOINT));
    }

    @Test
    void testCreateProviderStaticInstanceWithInvalidEndpoints() {
        when(peerForwarderConfiguration.getStaticEndpoints()).thenReturn(Arrays.asList(ENDPOINT, INVALID_ENDPOINT));

        assertThrows(IllegalStateException.class,
                () -> StaticPeerListProvider.createPeerListProvider(peerForwarderConfiguration, pluginMetrics));
    }

}