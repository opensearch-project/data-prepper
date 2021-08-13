package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.PeerForwarderConfig;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroupBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DnsPeerListProvider_CreateTest {

    private static final String PLUGIN_NAME = "PLUGIN_NAME";
    private static final String VALID_ENDPOINT = "VALID.ENDPOINT";
    private static final String INVALID_ENDPOINT = "INVALID_ENDPOINT_";
    private static final String PIPELINE_NAME = "pipelineName";

    @Mock
    private DnsAddressEndpointGroupBuilder dnsAddressEndpointGroupBuilder;
    @Mock
    private DnsAddressEndpointGroup dnsAddressEndpointGroup;
    
    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;

    private CompletableFuture completableFuture;

    @BeforeEach
    void setup() {
        pluginSetting = new PluginSetting(PLUGIN_NAME, new HashMap<>()) {{
            setPipelineName(PIPELINE_NAME);
        }};
        completableFuture = CompletableFuture.completedFuture(null);

        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.DNS.toString());
        pluginMetrics = mock(PluginMetrics.class);
    }

    @Test
    void testCreateProviderDnsInstance() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DOMAIN_NAME, VALID_ENDPOINT);

        when(dnsAddressEndpointGroupBuilder.build()).thenReturn(dnsAddressEndpointGroup);
        when(dnsAddressEndpointGroupBuilder.ttl(anyInt(), anyInt())).thenReturn(dnsAddressEndpointGroupBuilder);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);

        try (MockedStatic<DnsAddressEndpointGroup> armeriaMock = Mockito.mockStatic(DnsAddressEndpointGroup.class)) {
            armeriaMock.when(() -> DnsAddressEndpointGroup.builder(anyString())).thenReturn(dnsAddressEndpointGroupBuilder);

            PeerListProvider result = DnsPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics);

            assertThat(result, instanceOf(DnsPeerListProvider.class));
        }
    }

    @Test
    void testCreateProviderDnsInstanceWithNoHostname() {
        assertThrows(NullPointerException.class,
                () -> DnsPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics));

    }

    @Test
    void testCreateProviderDnsInstanceWithInvalidDomainName() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DOMAIN_NAME, INVALID_ENDPOINT);

        assertThrows(IllegalStateException.class,
                () -> DnsPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics));
    }

}