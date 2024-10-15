/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import org.opensearch.dataprepper.core.peerforwarder.ForwardingAuthentication;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * A class for creating Peer Forwarder server
 *
 * @since 2.0
 */
public class PeerForwarderHttpServerProvider implements Provider<Server> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderHttpServerProvider.class);

    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final CertificateProviderFactory certificateProviderFactory;
    private final PeerForwarderHttpService peerForwarderHttpService;

    public PeerForwarderHttpServerProvider(final PeerForwarderConfiguration peerForwarderConfiguration,
                                           final CertificateProviderFactory certificateProviderFactory,
                                           final PeerForwarderHttpService peerForwarderHttpService) {
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.certificateProviderFactory = certificateProviderFactory;
        this.peerForwarderHttpService = peerForwarderHttpService;
    }

    @Override
    public Server get() {
        final ServerBuilder sb = Server.builder();

        sb.disableServerHeader();

        if (peerForwarderConfiguration.isSsl()) {
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            LOG.info("Creating http source with SSL/TLS enabled.");
            // TODO: enable encrypted key with password
            sb.https(peerForwarderConfiguration.getServerPort())
                    .tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
            );

            if (peerForwarderConfiguration.getAuthentication() == ForwardingAuthentication.MUTUAL_TLS) {
                if (peerForwarderConfiguration.isSslFingerprintVerificationOnly()) {
                    final FingerprintTrustManagerFactory fingerprintTrustManagerFactory = new FingerprintTrustManagerFactory(certificate.getFingerprint());
                    sb.tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(fingerprintTrustManagerFactory)
                            .clientAuth(ClientAuth.REQUIRE));
                } else {
                    sb.tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(
                                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))
                            )
                            .clientAuth(ClientAuth.REQUIRE));
                }
            }
        } else {
            LOG.warn("Creating Peer Forwarder server without SSL/TLS. This is not secure.");
            sb.http(peerForwarderConfiguration.getServerPort());
        }


        sb.maxNumConnections(peerForwarderConfiguration.getMaxConnectionCount());
        sb.requestTimeout(Duration.ofMillis(peerForwarderConfiguration.getRequestTimeout()));
        final int threadCount = peerForwarderConfiguration.getServerThreadCount();
        final ScheduledThreadPoolExecutor blockingTaskExecutor = new ScheduledThreadPoolExecutor(threadCount);
        sb.blockingTaskExecutor(blockingTaskExecutor, true);
        // TODO: Add throttling service

        sb.annotatedService(PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI, peerForwarderHttpService);

        return sb.build();
    }
}