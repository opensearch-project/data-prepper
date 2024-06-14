/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.peerforwarder.codec.PeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpServerProvider;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServerProxy;
import org.opensearch.dataprepper.peerforwarder.server.ResponseHandler;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.inject.Named;


@Configuration
class PeerForwarderAppConfig {
    static final String COMPONENT_SCOPE = "core";
    static final String COMPONENT_ID = "peerForwarder";

    @Bean(name = "peerForwarderMetrics")
    public PluginMetrics pluginMetrics() {
        return PluginMetrics.fromNames(COMPONENT_ID, COMPONENT_SCOPE);
    }

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
            final CertificateProviderFactory certificateProviderFactory,
            @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics
    ) {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory, pluginMetrics);
    }

    @Bean
    public PeerForwarderClient peerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                                                   final PeerForwarderClientFactory peerForwarderClientFactory,
                                                   final PeerForwarderCodec peerForwarderCodec,
                                                   @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics
    ) {
        return new PeerForwarderClient(
                peerForwarderConfiguration, peerForwarderClientFactory, peerForwarderCodec, pluginMetrics);
    }

    @Bean(name = "defaultPeerForwarder")
    public DefaultPeerForwarderProvider peerForwarderProvider(final PeerForwarderClientFactory peerForwarderClientFactory,
                                                       final PeerForwarderClient peerForwarderClient,
                                                       final PeerForwarderConfiguration peerForwarderConfiguration,
                                                       @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics) {
        return new DefaultPeerForwarderProvider(peerForwarderClientFactory, peerForwarderClient, peerForwarderConfiguration, pluginMetrics);
    }

    @Bean
    @Primary
    public PeerForwarderProvider peerForwarderProvider(@Named("defaultPeerForwarder") final PeerForwarderProvider peerForwarderProvider) {
        return new LocalModePeerForwarderProvider(peerForwarderProvider);
    }

    @Bean
    public ResponseHandler responseHandler(@Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics) {
        return new ResponseHandler(pluginMetrics);
    }

    @Bean
    public PeerForwarderHttpService peerForwarderHttpService(
            final ResponseHandler responseHandler,
            final PeerForwarderProvider peerForwarderProvider,
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerForwarderCodec peerForwarderCodec,
            final AcknowledgementSetManager acknowledgementSetManager,
            @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics
    ) {
        return new PeerForwarderHttpService(responseHandler, peerForwarderProvider, peerForwarderConfiguration,
                peerForwarderCodec, acknowledgementSetManager, pluginMetrics);
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

    @Bean
    public PeerForwarderServer peerForwarderServer(
            final PeerForwarderHttpServerProvider peerForwarderHttpServerProvider,
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerForwarderProvider peerForwarderProvider) {
        return new PeerForwarderServerProxy(peerForwarderHttpServerProvider, peerForwarderConfiguration, peerForwarderProvider);
    }

}
