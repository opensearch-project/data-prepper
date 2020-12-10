package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeerListProviderFactoryTest {
    private static final String ENDPOINT = "ENDPOINT";

    @Mock
    private PluginSetting pluginSetting;

    private PeerListProviderFactory factory;

    @Before
    public void setup() {
        factory = new PeerListProviderFactory();
    }

    @Test
    public void testCreateProviderStaticInstanceNoEndpoints() {
        when(pluginSetting.getStringOrDefault(eq(PeerForwarderConfig.DISCOVERY_MODE), any())).thenReturn(DiscoveryMode.STATIC.toString());

        PeerListProvider result = factory.createProvider(pluginSetting);

        assertTrue(result instanceof StaticPeerListProvider);
        assertEquals(1, result.getPeerList().size());
        assertFalse(result.getPeerList().contains(ENDPOINT));
    }

    @Test
    public void testCreateProviderStaticInstanceWithEndpoints() {
        when(pluginSetting.getStringOrDefault(eq(PeerForwarderConfig.DISCOVERY_MODE), any())).thenReturn(DiscoveryMode.STATIC.toString());
        when(pluginSetting.getAttributeOrDefault(eq(PeerForwarderConfig.DATA_PREPPER_IPS), any())).thenReturn(Collections.singletonList(ENDPOINT));

        PeerListProvider result = factory.createProvider(pluginSetting);

        assertTrue(result instanceof StaticPeerListProvider);
        assertEquals(1, result.getPeerList().size());
        assertTrue(result.getPeerList().contains(ENDPOINT));
    }
}
