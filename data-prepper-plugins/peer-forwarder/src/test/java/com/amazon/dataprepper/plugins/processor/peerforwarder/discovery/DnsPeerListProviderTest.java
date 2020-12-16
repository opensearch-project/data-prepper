package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.amazon.dataprepper.plugins.processor.peerforwarder.HashRing;
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
import static org.mockito.Mockito.verify;
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
    @Mock
    private HashRing hashRing;

    private CompletableFuture completableFuture;

    private DnsPeerListProvider dnsPeerListProvider;

    @Before
    public void setup() {
        completableFuture = CompletableFuture.completedFuture(null);
        when(dnsAddressEndpointGroup.whenReady()).thenReturn(completableFuture);

        dnsPeerListProvider = new DnsPeerListProvider(dnsAddressEndpointGroup);
    }

    @Test(expected = NullPointerException.class)
    public void testDefaultListProviderWithNullHostname() {
        new DnsPeerListProvider(null);
    }

    @Test
    public void testGetPeerList() {
        when(dnsAddressEndpointGroup.endpoints()).thenReturn(ENDPOINT_LIST);

        List<String> results = dnsPeerListProvider.getPeerList();

        assertEquals(ENDPOINT_LIST.size(), results.size());
        assertTrue(results.contains(ENDPOINT_1));
        assertTrue(results.contains(ENDPOINT_2));
    }

    @Test
    public void testAddListener() {
        dnsPeerListProvider.addListener(hashRing);

        verify(dnsAddressEndpointGroup).addListener(hashRing);
    }

    @Test
    public void testRemoveListener() {
        dnsPeerListProvider.removeListener(hashRing);

        verify(dnsAddressEndpointGroup).removeListener(hashRing);
    }
}
