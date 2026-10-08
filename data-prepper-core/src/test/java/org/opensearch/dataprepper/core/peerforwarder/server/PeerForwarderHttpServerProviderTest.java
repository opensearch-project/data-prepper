/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderHttpServerProviderTest {
    @Mock
    PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    CertificateProviderFactory certificateProviderFactory;

    @Mock
    PeerForwarderHttpService peerForwarderHttpService;

    @Mock
    CircuitBreaker circuitBreaker;

    private PeerForwarderHttpServerProvider createObjectUnderTest() {
        return new PeerForwarderHttpServerProvider(peerForwarderConfiguration, certificateProviderFactory, peerForwarderHttpService);
    }

    @Test
    void get_should_create_a_server() {
        when(peerForwarderConfiguration.getMaxConnectionCount()).thenReturn(500);
        final Server server = createObjectUnderTest().get();

        Assertions.assertNotNull(server);
        assertThat(server, instanceOf(Server.class));
    }

    @Test
    void get_with_open_circuit_breaker_rejects_requests_with_429_before_the_service() {
        when(peerForwarderConfiguration.getMaxConnectionCount()).thenReturn(500);
        when(circuitBreaker.isOpen()).thenReturn(true);
        final Server server = new PeerForwarderHttpServerProvider(
                peerForwarderConfiguration, certificateProviderFactory, peerForwarderHttpService, circuitBreaker).get();

        server.start().join();
        try {
            final AggregatedHttpResponse response = WebClient.of("http://127.0.0.1:" + server.activeLocalPort())
                    .post(PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI, "{}")
                    .aggregate()
                    .join();

            assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
            verify(peerForwarderHttpService, never()).doPost(any());
        } finally {
            server.stop().join();
        }
    }

}