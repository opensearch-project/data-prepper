/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroupBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.discovery.DnsPeerListProvider;
import org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DnsPeerListProviderCreationTest {

    private static final String VALID_ENDPOINT = "VALID.ENDPOINT";
    private static final String INVALID_ENDPOINT = "INVALID_ENDPOINT_";

    @Mock
    private DnsAddressEndpointGroupBuilder dnsAddressEndpointGroupBuilder;
    @Mock
    private DnsAddressEndpointGroup dnsAddressEndpointGroup;

    private PeerForwarderConfiguration peerForwarderConfiguration;
    private PluginMetrics pluginMetrics;

    private CompletableFuture completableFuture;

    @BeforeEach
    void setup() {

        completableFuture = CompletableFuture.completedFuture(null);

        peerForwarderConfiguration = mock(PeerForwarderConfiguration.class);
        pluginMetrics = mock(PluginMetrics.class);
    }

    @Test
    void testCreateProviderDnsInstance() {
        when(peerForwarderConfiguration.getDomainName()).thenReturn(VALID_ENDPOINT);

        when(dnsAddressEndpointGroupBuilder.build()).thenReturn(dnsAddressEndpointGroup);
        when(dnsAddressEndpointGroupBuilder.ttl(anyInt(), anyInt())).thenReturn(dnsAddressEndpointGroupBuilder);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);

        try (MockedStatic<DnsAddressEndpointGroup> armeriaMock = Mockito.mockStatic(DnsAddressEndpointGroup.class)) {
            armeriaMock.when(() -> DnsAddressEndpointGroup.builder(anyString())).thenReturn(dnsAddressEndpointGroupBuilder);

            PeerListProvider result = DnsPeerListProvider.createPeerListProvider(peerForwarderConfiguration, pluginMetrics);

            assertThat(result, instanceOf(DnsPeerListProvider.class));
        }
    }

    @Test
    void testCreateProviderDnsInstanceWithNoHostname() {
        assertThrows(NullPointerException.class,
                () -> DnsPeerListProvider.createPeerListProvider(peerForwarderConfiguration, pluginMetrics));
    }

    @Test
    void testCreateProviderDnsInstanceWithInvalidDomainName() {
        when(peerForwarderConfiguration.getDomainName()).thenReturn(INVALID_ENDPOINT);

        assertThrows(IllegalStateException.class,
                () -> DnsPeerListProvider.createPeerListProvider(peerForwarderConfiguration, pluginMetrics));
    }

}