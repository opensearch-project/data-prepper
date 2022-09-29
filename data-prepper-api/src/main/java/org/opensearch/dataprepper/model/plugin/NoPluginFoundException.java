/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * An exception representing that Data Prepper was unable to
 * find a necessary plugin.
 *
 * @since 1.2
 */
public class NoPluginFoundException extends RuntimeException {
    public NoPluginFoundException(final String message) {
        super(message);
    }
}
