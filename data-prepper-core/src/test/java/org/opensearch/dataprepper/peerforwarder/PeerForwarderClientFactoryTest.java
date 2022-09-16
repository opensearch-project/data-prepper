/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import org.hamcrest.core.IsInstanceOf;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderClientFactoryTest {

    @Mock
    PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    PeerClientPool peerClientPool;

    @Mock
    CertificateProviderFactory certificateProviderFactory;

    private PeerForwarderClientFactory createObjectUnderTest() {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory);
    }

    @Test
    void testCreateHashRing_with_endpoints_should_return() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.STATIC);
        when(peerForwarderConfiguration.getStaticEndpoints()).thenReturn(Collections.singletonList("10.10.0.1"));

        HashRing hashRing = createObjectUnderTest().createHashRing();
        assertThat(hashRing, new IsInstanceOf(HashRing.class));
    }

    @Test
    void testCreateHashRing_without_endpoints_should_throw() {
        when(peerForwarderConfiguration.getDiscoveryMode()).thenReturn(DiscoveryMode.STATIC);

        PeerForwarderClientFactory peerForwarderClientFactory = createObjectUnderTest();

        assertThrows(RuntimeException.class, peerForwarderClientFactory::createHashRing);
    }

    @Test
    void testCreatePeerClientPool_should_return() {
        PeerForwarderClientFactory peerForwarderClientFactory = createObjectUnderTest();

        PeerClientPool returnedPeerClientPool = peerForwarderClientFactory.setPeerClientPool();

        assertThat(returnedPeerClientPool, equalTo(peerClientPool));
    }

    @ParameterizedTest
    @EnumSource(ForwardingAuthentication.class)
    void testCreatePeerClientPool_should_set_the_authentication(final ForwardingAuthentication authentication) {
        when(peerForwarderConfiguration.getAuthentication()).thenReturn(authentication);
        createObjectUnderTest().setPeerClientPool();
        verify(peerClientPool).setAuthentication(authentication);
    }
}