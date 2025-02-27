/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * This exception is thrown when a plugin in configured with an invalid
 * configuration.
 *
 * @since 1.3
 */
public class InvalidPipelineConfigurationException extends RuntimeException {
    public InvalidPipelineConfigurationException(final String message) {
        super(message);
    }
}
