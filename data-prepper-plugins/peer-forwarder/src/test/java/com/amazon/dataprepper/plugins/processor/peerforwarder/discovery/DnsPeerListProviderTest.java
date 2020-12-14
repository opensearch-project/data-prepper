package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.endpoint.dns.DnsAddressEndpointGroup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DnsPeerListProviderTest {
    private static final String ENDPOINT_1 = "10.1.1.1";
    private static final String ENDPOINT_2 = "10.1.1.2";
    private static final List<Endpoint> ENDPOINT_LIST = Arrays.asList(
            Endpoint.of(ENDPOINT_1),
            Endpoint.of(ENDPOINT_2)
    );

    @Mock
    private DnsAddressEndpointGroup dnsAddressEndpointGroup;

    private CompletableFuture completableFuture;

    @Before
    public void setup() {
        completableFuture = CompletableFuture.completedFuture(null);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);
    }

    @Test(expected = NullPointerException.class)
    public void testDefaultListProviderWithNullHostname() {
        DnsPeerListProvider listProvider = new DnsPeerListProvider(null);
    }

    @Test
    public void testGetPeerList() {
        DnsPeerListProvider listProvider = new DnsPeerListProvider(dnsAddressEndpointGroup);

        when(dnsAddressEndpointGroup.endpoints()).thenReturn(ENDPOINT_LIST);

        List<String> results = listProvider.getPeerList();

        assertEquals(ENDPOINT_LIST.size(), results.size());
        assertTrue(results.contains(ENDPOINT_1));
        assertTrue(results.contains(ENDPOINT_2));
    }
}
