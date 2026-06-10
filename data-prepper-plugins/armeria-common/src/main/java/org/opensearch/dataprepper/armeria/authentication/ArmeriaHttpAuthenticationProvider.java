/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.armeria.authentication;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An interface for providing authentication in Armeria-based HTTP servers.
 * <p>
 * Plugin authors can use this interface for Armeria authentication in
 * HTTP servers.
 *
 * @since 1.2
 */
public interface ArmeriaHttpAuthenticationProvider {
    /**
     * The plugin name for the plugin which allows unauthenticated
     * requests. This plugin will disable authentication.
     */
    String UNAUTHENTICATED_PLUGIN_NAME = "unauthenticated";

    /**
     * Gets an authentication decorator to an Armeria {@link ServerBuilder}.
     *
     * @since 2.0
     * @return returns authentication decorator
     */
    default Optional<Function<? super HttpService, ? extends HttpService>> getAuthenticationDecorator() {
        return Optional.empty();
    }

    /**
     * Gets a TLS customizer for configuring client authentication (e.g., mTLS).
     * Called during server setup when SSL is enabled.
     *
     * @return an optional consumer that configures the {@link SslContextBuilder}
     */
    default Optional<Consumer<SslContextBuilder>> getTlsCustomizer() {
        return Optional.empty();
    }
}
