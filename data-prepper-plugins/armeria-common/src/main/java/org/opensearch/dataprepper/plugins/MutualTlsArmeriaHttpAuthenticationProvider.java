/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import io.netty.handler.ssl.ClientAuth;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.ClientAuthConfiguration;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.Objects;
import java.util.Optional;

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
        if (!Files.exists(trustCertPath)) {
            throw new IllegalArgumentException(
                    "ssl_trust_certificate_file does not exist: " + trustCertPath);
        }
        if (!Files.isReadable(trustCertPath)) {
            throw new IllegalArgumentException(
                    "ssl_trust_certificate_file is not readable: " + trustCertPath);
        }

        this.trustManagerFactory = buildTrustManagerFactory(config.getSslTrustCertificateFile());
        LOG.info("mutual_tls authentication configured with trust CA: {}", config.getSslTrustCertificateFile());
    }

    @Override
    public Optional<ClientAuthConfiguration> getClientAuthConfiguration() {
        return Optional.of(new ClientAuthConfiguration() {
            @Override
            public ClientAuth getClientAuth() {
                return ClientAuth.REQUIRE;
            }

            @Override
            public TrustManagerFactory getTrustManagerFactory() {
                return trustManagerFactory;
            }
        });
    }

    private TrustManagerFactory buildTrustManagerFactory(final String trustCertFile) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);

            try (FileInputStream fis = new FileInputStream(trustCertFile)) {
                int certIndex = 0;
                for (java.security.cert.Certificate cert : cf.generateCertificates(fis)) {
                    trustStore.setCertificateEntry("ca-" + certIndex++, cert);
                }
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            return tmf;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to load trust certificate from " + trustCertFile + ": " + e.getMessage(), e);
        }
    }
}
