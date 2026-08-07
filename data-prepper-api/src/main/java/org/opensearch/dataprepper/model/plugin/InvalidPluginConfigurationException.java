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
 * This exception is thrown when a plugin in configured with an invalid
 * configuration.
 *
 * @since 1.3
 */
public class InvalidPluginConfigurationException extends RuntimeException {
    public InvalidPluginConfigurationException(final String message) {
        super(message);
    }

    public InvalidPluginConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
