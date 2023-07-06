/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.armeria.authentication;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServerBuilder;

import java.util.Optional;
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
}
