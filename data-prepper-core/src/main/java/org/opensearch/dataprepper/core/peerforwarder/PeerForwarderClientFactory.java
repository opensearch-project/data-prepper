/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;


import org.opensearch.dataprepper.core.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;

public class PeerForwarderClientFactory {
    public static final int NUM_VIRTUAL_NODES = 128;

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerClientPool peerClientPool;
    private final CertificateProviderFactory certificateProviderFactory;
    private final PluginMetrics pluginMetrics;

    public PeerForwarderClientFactory(final PeerForwarderConfiguration peerForwarderConfiguration,
                                      final PeerClientPool peerClientPool,
                                      final CertificateProviderFactory certificateProviderFactory,
                                      final PluginMetrics pluginMetrics) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerClientPool = peerClientPool;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pluginMetrics = pluginMetrics;
    }

    public HashRing createHashRing() {
        final DiscoveryMode discoveryMode = peerForwarderConfiguration.getDiscoveryMode();
        final PeerListProvider peerListProvider = discoveryMode.create(peerForwarderConfiguration, pluginMetrics);
        return new HashRing(peerListProvider, NUM_VIRTUAL_NODES);
    }

    public PeerClientPool setPeerClientPool() {
        peerClientPool.setClientTimeoutMillis(peerForwarderConfiguration.getClientTimeout());

        final int targetPort = peerForwarderConfiguration.getServerPort();
        peerClientPool.setPort(targetPort);

        final boolean ssl = peerForwarderConfiguration.isSsl();
        final boolean useAcmCertForSsl = peerForwarderConfiguration.isUseAcmCertificateForSsl();

        peerClientPool.setAuthentication(peerForwarderConfiguration.getAuthentication());

        if (ssl || useAcmCertForSsl) {
            peerClientPool.setSsl(true);

            peerClientPool.setSslDisableVerification(peerForwarderConfiguration.isSslDisableVerification());
            peerClientPool.setSslFingerprintVerificationOnly(peerForwarderConfiguration.isSslFingerprintVerificationOnly());
            peerClientPool.setCertificate(certificateProviderFactory.getCertificateProvider().getCertificate());
        }

        return peerClientPool;
    }
}
