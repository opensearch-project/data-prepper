/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.core.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PeerForwarderAppConfigTest {

    private static final PeerForwarderAppConfig peerForwarderAppConfig = new PeerForwarderAppConfig();

    @Test
    void peerForwarderConfiguration_with_non_null_DataPrepperConfiguration_should_return_PeerForwarderConfiguration() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        when(dataPrepperConfiguration.getPeerForwarderConfiguration()).thenReturn(mock(PeerForwarderConfiguration.class));

        final PeerForwarderConfiguration peerForwarderConfiguration = peerForwarderAppConfig.peerForwarderConfiguration(dataPrepperConfiguration);

        verify(dataPrepperConfiguration, times(2)).getPeerForwarderConfiguration();
        assertThat(peerForwarderConfiguration, notNullValue());
    }

    @Test
    void peerForwarderConfiguration_with_null_DataPrepperConfiguration_should_return_default_PeerForwarderConfiguration() {
        final DataPrepperConfiguration dataPrepperConfiguration = null;
        final PeerForwarderConfiguration peerForwarderConfiguration = peerForwarderAppConfig.peerForwarderConfiguration(dataPrepperConfiguration);

        assertThat(peerForwarderConfiguration, notNullValue());
    }

    @Test
    void peerClientPool_should_return_test() {
        PeerClientPool peerClientPool = peerForwarderAppConfig.peerClientPool();

        assertThat(peerClientPool, notNullValue());
    }

    @Test
    void peerForwarderClientFactory_should_return_test() {
        PeerForwarderClientFactory peerForwarderClientFactory = peerForwarderAppConfig.peerForwarderClientFactory(
                mock(PeerForwarderConfiguration.class),
                mock(PeerClientPool.class),
                mock(CertificateProviderFactory.class),
                mock(PluginMetrics.class)
        );

        assertThat(peerForwarderClientFactory, notNullValue());
    }

    @Test
    void peerForwarderHttpServerProvider_passes_circuit_breaker_so_the_server_rejects_requests_while_it_is_open() throws Exception {
        final PeerForwarderConfiguration peerForwarderConfiguration = mock(PeerForwarderConfiguration.class);
        when(peerForwarderConfiguration.getMaxConnectionCount()).thenReturn(500);
        final PeerForwarderHttpService peerForwarderHttpService = mock(PeerForwarderHttpService.class);
        final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
        when(circuitBreaker.isOpen()).thenReturn(true);

        final Server server = peerForwarderAppConfig.peerForwarderHttpServerProvider(
                peerForwarderConfiguration,
                mock(CertificateProviderFactory.class),
                peerForwarderHttpService,
                circuitBreaker
        ).get();

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