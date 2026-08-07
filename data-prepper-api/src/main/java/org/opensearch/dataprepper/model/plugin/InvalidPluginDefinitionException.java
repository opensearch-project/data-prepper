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
 * This exception indicates that a plugin has an invalid definition. This
 * may mean that it does not have a required constructor, is not a concrete
 * class, or not available for construction due to class access.
 *
 * @since 1.2
 */
public class InvalidPluginDefinitionException extends RuntimeException {
    public InvalidPluginDefinitionException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidPluginDefinitionException(final String message) {
        super(message);
    }
}
