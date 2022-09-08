/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpServerProvider;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.ResponseHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class PeerForwarderAppConfig {

    @Bean
    public PeerForwarderConfiguration peerForwarderConfiguration(
            @Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getPeerForwarderConfiguration() != null) {
                return dataPrepperConfiguration.getPeerForwarderConfiguration();
            }
        else
            return new PeerForwarderConfiguration();
    }

    @Bean
    public PeerClientPool peerClientPool() {
        return new PeerClientPool();
    }

    @Bean
    public CertificateProviderFactory certificateProviderFactory(final PeerForwarderConfiguration peerForwarderConfiguration) {
        return new CertificateProviderFactory(peerForwarderConfiguration);
    }

    @Bean
    public PeerForwarderClientFactory peerForwarderClientFactory(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerClientPool peerClientPool,
            final CertificateProviderFactory certificateProviderFactory
    ) {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory);
    }

    @Bean
    public PeerForwarderClient peerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                                                   final PeerForwarderClientFactory peerForwarderClientFactory,
                                                   final ObjectMapper objectMapper
    ) {
        return new PeerForwarderClient(peerForwarderConfiguration, peerForwarderClientFactory, objectMapper);
    }

    @Bean
    public PeerForwarderProvider peerForwarderProvider(final PeerForwarderClientFactory peerForwarderClientFactory,
                                                       final PeerForwarderClient peerForwarderClient) {
        return new PeerForwarderProvider(peerForwarderClientFactory, peerForwarderClient);
    }
    @Bean
    public ResponseHandler responseHandler() {
        return new ResponseHandler();
    }

    @Bean
    public PeerForwarderHttpService peerForwarderHttpService(
            final ResponseHandler responseHandler,
            final ObjectMapper objectMapper
    ) {
        return new PeerForwarderHttpService(responseHandler, objectMapper);
    }

    @Bean
    public PeerForwarderHttpServerProvider peerForwarderHttpServerProvider(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory,
            final PeerForwarderHttpService peerForwarderHttpService
    ) {
        return new PeerForwarderHttpServerProvider(peerForwarderConfiguration,
                certificateProviderFactory, peerForwarderHttpService);
    }

    @Bean(name="peerForwarderServer")
    public Server server(
            final PeerForwarderHttpServerProvider peerForwarderHttpServerProvider
    ) {
        return peerForwarderHttpServerProvider.get();
    }

    @Bean
    public PeerForwarderServer peerForwarderServer(
            @Qualifier("peerForwarderServer") final Server peerForwarderServer,
            final PeerForwarderConfiguration peerForwarderConfiguration) {
        return new PeerForwarderServer(peerForwarderConfiguration, peerForwarderServer);
    }

}
