/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.auth.AuthService;
import io.netty.handler.ssl.ClientAuth;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.ClientAuthConfiguration;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The plugin for mutual TLS (client certificate) authentication on Armeria HTTP servers.
 *
 * @since 2.18
 */
@Experimental
@DataPrepperPlugin(name = "mutual_tls",
        pluginType = ArmeriaHttpAuthenticationProvider.class,
        pluginConfigurationType = MutualTlsAuthenticationConfig.class)
public class MutualTlsArmeriaHttpAuthenticationProvider implements ArmeriaHttpAuthenticationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(MutualTlsArmeriaHttpAuthenticationProvider.class);

    private final TrustManagerFactory trustManagerFactory;

    @DataPrepperPluginConstructor
    public MutualTlsArmeriaHttpAuthenticationProvider(final MutualTlsAuthenticationConfig config) {
        Objects.requireNonNull(config, "mutual_tls authentication config must not be null");
        Objects.requireNonNull(config.getSslTrustCertificateFile(),
                "ssl_trust_certificate_file is required for mutual_tls authentication");

        final Path trustCertPath = Path.of(config.getSslTrustCertificateFile());
        if (!Files.isRegularFile(trustCertPath) || !Files.isReadable(trustCertPath)) {
            throw new IllegalArgumentException(
                    "ssl_trust_certificate_file must be a readable regular file: " + trustCertPath);
        }

        this.trustManagerFactory = buildTrustManagerFactory(config.getSslTrustCertificateFile());
        LOG.info("mutual_tls authentication configured with trust CA: {}", config.getSslTrustCertificateFile());
    }

    /**
     * Rejects any request whose TLS session has no verified client certificate with HTTP 401.
     * This keeps the plugin fail-closed even if a source does not apply
     * {@link #getClientAuthConfiguration()} to its TLS context.
     */
    @Override
    public Optional<Function<? super HttpService, ? extends HttpService>> getAuthenticationDecorator() {
        return Optional.of(AuthService.builder()
                .add(MutualTlsArmeriaHttpAuthenticationProvider::hasVerifiedClientCertificate)
                .newDecorator());
    }

    static CompletionStage<Boolean> hasVerifiedClientCertificate(final ServiceRequestContext context,
                                                                 final HttpRequest request) {
        final SSLSession sslSession = context.sslSession();
        if (sslSession == null) {
            return CompletableFuture.completedFuture(false);
        }
        try {
            return CompletableFuture.completedFuture(sslSession.getPeerCertificates().length > 0);
        } catch (final SSLPeerUnverifiedException e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public Optional<ClientAuthConfiguration> getClientAuthConfiguration() {
        return Optional.of(new ClientAuthConfiguration() {
            /*
             * OPTIONAL instead of REQUIRE: a client without a certificate still completes the
             * handshake and then receives 401 from the authentication decorator. This keeps
             * unauthenticated_health_check working. A presented certificate that is not signed
             * by a trusted CA is still rejected during the handshake.
             */
            @Override
            public ClientAuth getClientAuth() {
                return ClientAuth.OPTIONAL;
            }

            @Override
            public TrustManagerFactory getTrustManagerFactory() {
                return trustManagerFactory;
            }
        });
    }

    static TrustManagerFactory buildTrustManagerFactory(final String trustCertFile) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);

            try (FileInputStream fis = new FileInputStream(trustCertFile)) {
                final Collection<? extends java.security.cert.Certificate> certificates =
                        cf.generateCertificates(fis);
                if (certificates.isEmpty()) {
                    throw new IllegalArgumentException(
                            "ssl_trust_certificate_file contains no certificates: " + trustCertFile);
                }
                int certIndex = 0;
                for (final java.security.cert.Certificate cert : certificates) {
                    trustStore.setCertificateEntry("ca-" + certIndex++, cert);
                }
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            return tmf;
        } catch (final IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to load trust certificate from " + trustCertFile + ": " + e.getMessage(), e);
        }
    }
}
