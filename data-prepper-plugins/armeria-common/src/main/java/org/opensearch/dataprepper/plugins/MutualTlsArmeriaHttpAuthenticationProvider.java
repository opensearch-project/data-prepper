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
import io.netty.handler.ssl.SslContextBuilder;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "mutual_tls",
        pluginType = ArmeriaHttpAuthenticationProvider.class,
        pluginConfigurationType = MutualTlsAuthenticationConfig.class)
public class MutualTlsArmeriaHttpAuthenticationProvider implements ArmeriaHttpAuthenticationProvider {

    private final MutualTlsAuthenticationConfig config;

    @DataPrepperPluginConstructor
    public MutualTlsArmeriaHttpAuthenticationProvider(final MutualTlsAuthenticationConfig config) {
        Objects.requireNonNull(config, "mutual_tls authentication config must not be null");
        Objects.requireNonNull(config.getSslTrustCertificateFile(),
                "ssl_trust_certificate_file is required for mutual_tls authentication");
        this.config = config;
    }

    @Override
    public Optional<Consumer<SslContextBuilder>> getTlsCustomizer() {
        return Optional.of(sslContextBuilder -> {
            sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
            sslContextBuilder.trustManager(new File(config.getSslTrustCertificateFile()));
        });
    }
}
