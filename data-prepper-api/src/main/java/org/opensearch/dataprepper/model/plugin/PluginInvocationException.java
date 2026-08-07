/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
