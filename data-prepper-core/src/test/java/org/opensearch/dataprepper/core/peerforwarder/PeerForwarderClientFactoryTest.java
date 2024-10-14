/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.ForwardingAuthentication;
import org.opensearch.dataprepper.core.peerforwarder.HashRing;
import org.opensearch.dataprepper.core.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderClientFactory;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.core.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
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

    @Mock
    PluginMetrics pluginMetrics;

    private PeerForwarderClientFactory createObjectUnderTest() {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory, pluginMetrics);
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

    @Nested
    class WithSsl {

        @Mock
        private CertificateProvider certificateProvider;

        @Mock
        private Certificate certificate;

        @BeforeEach
        void setUp() {
            when(peerForwarderConfiguration.isSsl()).thenReturn(true);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);

            when(certificateProvider.getCertificate()).thenReturn(certificate);
        }

        @ParameterizedTest
        @ValueSource(booleans = { true, false })
        void setPeerClientPool_should_supply_sslDisableVerification_when_ssl_true(final boolean sslDisableVerification) {
            when(peerForwarderConfiguration.isSslDisableVerification())
                    .thenReturn(sslDisableVerification);


            createObjectUnderTest().setPeerClientPool();

            verify(peerClientPool).setSslDisableVerification(sslDisableVerification);
        }

        @ParameterizedTest
        @ValueSource(booleans = { true, false })
        void setPeerClientPool_should_supply_sslFingerprintVerificationOnly_when_ssl_true(final boolean sslFingerprintVerificationOnly) {
            when(peerForwarderConfiguration.isSslFingerprintVerificationOnly())
                    .thenReturn(sslFingerprintVerificationOnly);


            createObjectUnderTest().setPeerClientPool();

            verify(peerClientPool).setSslFingerprintVerificationOnly(sslFingerprintVerificationOnly);
        }
    }

    @Test
    void setPeerClientPool_should_not_supply_sslDisableVerification_when_ssl_false() {
        createObjectUnderTest().setPeerClientPool();

        verify(peerClientPool, never()).setSslDisableVerification(anyBoolean());
    }

    @Test
    void setPeerClientPool_should_not_supply_sslFingerprintVerificationOnly_when_ssl_false() {
        createObjectUnderTest().setPeerClientPool();

        verify(peerClientPool, never()).setSslFingerprintVerificationOnly(anyBoolean());
    }

    @ParameterizedTest
    @EnumSource(ForwardingAuthentication.class)
    void testCreatePeerClientPool_should_set_the_authentication(final ForwardingAuthentication authentication) {
        when(peerForwarderConfiguration.getAuthentication()).thenReturn(authentication);
        createObjectUnderTest().setPeerClientPool();
        verify(peerClientPool).setAuthentication(authentication);
    }
}