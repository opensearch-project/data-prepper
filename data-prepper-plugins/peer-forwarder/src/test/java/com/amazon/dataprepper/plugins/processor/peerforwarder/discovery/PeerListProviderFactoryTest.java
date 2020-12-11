package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class PeerListProviderFactoryTest {
    private static final String PLUGIN_NAME = "PLUGIN_NAME";
    private static final String ENDPOINT = "ENDPOINT";

    private PluginSetting pluginSetting;

    private PeerListProviderFactory factory;

    @Before
    public void setup() {
        factory = new PeerListProviderFactory();
        pluginSetting = new PluginSetting(PLUGIN_NAME, new HashMap<>());
    }

    @Test
    public void testCreateProviderStaticInstanceNoEndpoints() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());

        PeerListProvider result = factory.createProvider(pluginSetting);

        assertTrue(result instanceof StaticPeerListProvider);
        assertEquals(1, result.getPeerList().size());
        assertFalse(result.getPeerList().contains(ENDPOINT));
    }

    @Test
    public void testCreateProviderStaticInstanceWithEndpoints() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());
        pluginSetting.getSettings().put(PeerForwarderConfig.DATA_PREPPER_IPS, Collections.singletonList(ENDPOINT));

        PeerListProvider result = factory.createProvider(pluginSetting);

        assertTrue(result instanceof StaticPeerListProvider);
        assertEquals(1, result.getPeerList().size());
        assertTrue(result.getPeerList().contains(ENDPOINT));
    }
}
