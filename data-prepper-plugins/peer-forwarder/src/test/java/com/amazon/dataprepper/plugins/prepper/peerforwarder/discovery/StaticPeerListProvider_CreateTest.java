package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.PeerForwarderConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class StaticPeerListProvider_CreateTest {

    private static final String PLUGIN_NAME = "PLUGIN_NAME";
    private static final String ENDPOINT = "ENDPOINT";
    private static final String INVALID_ENDPOINT = "INVALID_ENDPOINT_";
    private static final String PIPELINE_NAME = "pipelineName";

    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;

    @BeforeEach
    void setup() {
        pluginSetting = new PluginSetting(PLUGIN_NAME, new HashMap<>()){{ setPipelineName(PIPELINE_NAME); }};

        pluginMetrics = mock(PluginMetrics.class);
    }

    @Test
    void testCreateProviderStaticInstanceNoEndpoints() {

        assertThrows(NullPointerException.class,
                () -> StaticPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics));
    }

    @Test
    void testCreateProviderStaticInstanceWithEndpoints() {
        pluginSetting.getSettings().put(PeerForwarderConfig.STATIC_ENDPOINTS, Collections.singletonList(ENDPOINT));

        PeerListProvider result = StaticPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics);

        assertThat(result, instanceOf(StaticPeerListProvider.class));
        assertEquals(1, result.getPeerList().size());
        assertTrue(result.getPeerList().contains(ENDPOINT));
    }

    @Test
    void testCreateProviderStaticInstanceWithInvalidEndpoints() {
        pluginSetting.getSettings().put(PeerForwarderConfig.STATIC_ENDPOINTS, Arrays.asList(ENDPOINT, INVALID_ENDPOINT));

        assertThrows(IllegalStateException.class,
                () -> StaticPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics));
    }

}