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
