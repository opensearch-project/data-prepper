/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.exception;

/**
 * Exception class for Office 365 source plugin errors.
 */
public class Office365Exception extends RuntimeException {
    private final boolean retryable;

    public Office365Exception(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public Office365Exception(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
