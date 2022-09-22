/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;


import com.amazon.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.peerforwarder.discovery.PeerListProvider;

public class PeerForwarderClientFactory {
    public static final int NUM_VIRTUAL_NODES = 128;

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerClientPool peerClientPool;
    private final CertificateProviderFactory certificateProviderFactory;

    public PeerForwarderClientFactory(final PeerForwarderConfiguration peerForwarderConfiguration,
                                      final PeerClientPool peerClientPool,
                                      final CertificateProviderFactory certificateProviderFactory) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerClientPool = peerClientPool;
        this.certificateProviderFactory = certificateProviderFactory;
    }

    public HashRing createHashRing(final PluginMetrics pluginMetrics) {
        final DiscoveryMode discoveryMode = peerForwarderConfiguration.getDiscoveryMode();
        final PeerListProvider peerListProvider = discoveryMode.create(peerForwarderConfiguration, pluginMetrics);
        return new HashRing(peerListProvider, NUM_VIRTUAL_NODES);
    }

    public PeerClientPool setPeerClientPool() {
        peerClientPool.setClientTimeoutSeconds(3);

        final int targetPort = peerForwarderConfiguration.getServerPort();
        peerClientPool.setPort(targetPort);

        final boolean ssl = peerForwarderConfiguration.isSsl();
        final boolean useAcmCertForSsl = peerForwarderConfiguration.isUseAcmCertificateForSsl();

        peerClientPool.setAuthentication(peerForwarderConfiguration.getAuthentication());

        if (ssl || useAcmCertForSsl) {
            peerClientPool.setSsl(true);

            peerClientPool.setSslDisableVerification(peerForwarderConfiguration.isSslDisableVerification());
            peerClientPool.setCertificate(certificateProviderFactory.getCertificateProvider().getCertificate());
        }

        return peerClientPool;
    }
}
