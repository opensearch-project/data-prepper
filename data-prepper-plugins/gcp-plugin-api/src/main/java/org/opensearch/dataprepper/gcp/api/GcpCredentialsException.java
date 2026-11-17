/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

/**
 * Thrown by {@link GcpCredentialsSupplier#getCredentials(GcpCredentialsOptions)} when Google credentials cannot be constructed.
 */
public class GcpCredentialsException extends RuntimeException {
    /**
     * @param message the failure message, naming the auth mode
     * @param cause   the underlying failure
     */
    public GcpCredentialsException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
