package com.amazon.dataprepper.armeria.authentication;

import com.linecorp.armeria.server.ServerBuilder;

/**
 * An interface for providing authentication in Armeria-based HTTP servers.
 * <p>
 * Plugin authors can use this interface for Armeria authentication in
 * HTTP servers.
 *
 * @since 1.2
 */
public interface ArmeriaAuthenticationProvider {
    /**
     * The plugin name for the plugin which allows unauthenticated
     * requests. This plugin will disable authentication.
     */
    String UNAUTHENTICATED_PLUGIN_NAME = "unauthenticated";

    /**
     * Adds an authentication decorator to an Armeria {@link ServerBuilder}.
     *
     * @param serverBuilder the builder for the server needing authentication
     * @since 1.2
     */
    void addAuthenticationDecorator(ServerBuilder serverBuilder);
}
