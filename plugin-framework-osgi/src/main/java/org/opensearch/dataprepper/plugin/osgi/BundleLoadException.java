/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

/**
 * Thrown when a bundle fails to install, resolve, or start during the static
 * bundle loading sequence. Contains a human-readable translated error message.
 */
public class BundleLoadException extends RuntimeException {

    public BundleLoadException(final String message) {
        super(message);
    }

    public BundleLoadException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
