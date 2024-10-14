/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.Authenticator;

/**
 * Pluggable interface for authentication of core Data Prepper APIs.
 *
 * @since 1.2
 */
public interface DataPrepperCoreAuthenticationProvider {
    /**
     * The plugin name for the plugin which allows unauthenticated
     * requests. This plugin will disable authentication.
     *
     * @since 1.2
     */
    String UNAUTHENTICATED_PLUGIN_NAME = "unauthenticated";

    /**
     * Gets the Sun HTTP Server {@link Authenticator}.
     *
     * @return the authenticator instance
     * @since 1.2
     */
    Authenticator getAuthenticator();
}
