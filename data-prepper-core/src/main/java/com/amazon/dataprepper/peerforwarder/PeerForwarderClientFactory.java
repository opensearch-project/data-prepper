/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import com.amazon.dataprepper.peerforwarder.discovery.DiscoveryMode;
import com.amazon.dataprepper.peerforwarder.discovery.PeerListProvider;

import javax.inject.Inject;
import javax.inject.Named;

@Named
public class PeerForwarderClientFactory {
    public static final int NUM_VIRTUAL_NODES = 128;

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final PeerClientPool peerClientPool;

    @Inject
    public PeerForwarderClientFactory(PeerForwarderConfiguration peerForwarderConfiguration,
                                      PeerClientPool peerClientPool) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.peerClientPool = peerClientPool;
    }

    public HashRing createHashRing() {
        final DiscoveryMode discoveryMode = peerForwarderConfiguration.getDiscoveryMode();
        final PeerListProvider peerListProvider = discoveryMode.create(peerForwarderConfiguration);
        return new HashRing(peerListProvider, NUM_VIRTUAL_NODES);
    }

    public PeerClientPool setPeerClientPool() {
        peerClientPool.setClientTimeoutSeconds(3);

        final int targetPort = peerForwarderConfiguration.getServerPort();
        peerClientPool.setPort(targetPort);

        final boolean ssl = peerForwarderConfiguration.isSsl();
        final boolean useAcmCertForSsl = peerForwarderConfiguration.isUseAcmCertificateForSsl();

        if (ssl || useAcmCertForSsl) {
            peerClientPool.setSsl(true);

            final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(peerForwarderConfiguration);
            peerClientPool.setCertificate(certificateProviderFactory.getCertificateProvider().getCertificate());
        }

        return peerClientPool;
    }
}
