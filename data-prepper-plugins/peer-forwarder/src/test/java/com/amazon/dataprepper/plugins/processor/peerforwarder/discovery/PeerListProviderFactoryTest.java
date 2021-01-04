package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.processor.peerforwarder.PeerForwarderConfig;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroupBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeerListProviderFactoryTest {
    private static final String PLUGIN_NAME = "PLUGIN_NAME";
    private static final String ENDPOINT = "ENDPOINT";
    private static final String PIPELINE_NAME = "pipelineName";

    @Mock
    private DnsAddressEndpointGroupBuilder dnsAddressEndpointGroupBuilder;
    @Mock
    private DnsAddressEndpointGroup dnsAddressEndpointGroup;

    private CompletableFuture completableFuture;

    private PluginSetting pluginSetting;

    private PeerListProviderFactory factory;

    @Before
    public void setup() {
        factory = new PeerListProviderFactory();
        pluginSetting = new PluginSetting(PLUGIN_NAME, new HashMap<>()){{ setPipelineName(PIPELINE_NAME); }};
        completableFuture = CompletableFuture.completedFuture(null);
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedNoDiscoveryMode() {
        factory.createProvider(pluginSetting);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUndefinedDiscoveryModeEnum() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, "GARBAGE");

        factory.createProvider(pluginSetting);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedDiscoveryModeEnum() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.UNSUPPORTED.toString());

        factory.createProvider(pluginSetting);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProviderStaticInstanceNoEndpoints() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());

        factory.createProvider(pluginSetting);
    }

    @Test
    public void testCreateProviderStaticInstanceWithEndpoints() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());
        pluginSetting.getSettings().put(PeerForwarderConfig.STATIC_ENDPOINTS, Collections.singletonList(ENDPOINT));

        PeerListProvider result = factory.createProvider(pluginSetting);

        assertTrue(result instanceof StaticPeerListProvider);
        assertEquals(1, result.getPeerList().size());
        assertTrue(result.getPeerList().contains(ENDPOINT));
    }

    @Test
    public void testCreateProviderDnsInstance() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.DNS.toString());
        pluginSetting.getSettings().put(PeerForwarderConfig.HOSTNAME_FOR_DNS_LOOKUP, ENDPOINT);

        when(dnsAddressEndpointGroupBuilder.build()).thenReturn(dnsAddressEndpointGroup);
        when(dnsAddressEndpointGroupBuilder.ttl(anyInt(), anyInt())).thenReturn(dnsAddressEndpointGroupBuilder);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);

        try (MockedStatic<DnsAddressEndpointGroup> armeriaMock = Mockito.mockStatic(DnsAddressEndpointGroup.class)) {
            armeriaMock.when(() -> DnsAddressEndpointGroup.builder(anyString())).thenReturn(dnsAddressEndpointGroupBuilder);

            PeerListProvider result = factory.createProvider(pluginSetting);

            assertTrue(result instanceof DnsPeerListProvider);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProviderDnsInstanceWithNoHostname() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.DNS.toString());

        factory.createProvider(pluginSetting);
    }
}
