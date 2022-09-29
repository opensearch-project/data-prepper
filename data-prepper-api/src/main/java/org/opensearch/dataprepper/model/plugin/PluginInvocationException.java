/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * An exception thrown when a plugin class invocation fails. This may happen
 * if the plugin's constructor throws an exception.
 *
 * @since 1.2
 */
public class PluginInvocationException extends RuntimeException {
    public PluginInvocationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
