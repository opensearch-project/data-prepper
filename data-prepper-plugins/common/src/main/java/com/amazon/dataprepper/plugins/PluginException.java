/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

/**
 * Exceptions for old plugin system.
 *
 * @deprecated in 1.2.
 */
@Deprecated
public class PluginException extends RuntimeException {
    public PluginException(final Throwable cause) {
        super(cause);
    }

    public PluginException(final String message) {
        super(message);
    }

    public PluginException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
