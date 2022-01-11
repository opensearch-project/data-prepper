/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.plugin;

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
}
