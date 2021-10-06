/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
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
