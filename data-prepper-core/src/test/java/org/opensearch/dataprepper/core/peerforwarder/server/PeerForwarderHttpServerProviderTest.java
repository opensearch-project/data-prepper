/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
    void get_with_open_circuit_breaker_rejects_requests_with_429_before_the_service() throws Exception {
        when(peerForwarderConfiguration.getMaxConnectionCount()).thenReturn(500);
        when(circuitBreaker.isOpen()).thenReturn(true);
        final Server server = new PeerForwarderHttpServerProvider(
                peerForwarderConfiguration, certificateProviderFactory, peerForwarderHttpService, circuitBreaker).get();

        server.start().join();
        try {
            final HttpRequest request = HttpRequest.newBuilder(URI.create(
                            "http://127.0.0.1:" + server.activeLocalPort() + PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI))
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            final HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode(), equalTo(429));
            verify(peerForwarderHttpService, never()).doPost(any());
        } finally {
            server.stop().join();
        }
    }

}