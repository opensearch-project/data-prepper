/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.exception;

/**
 * Exception to indicate unauthorized access.
 * It could either be caused by invalid credentials supplied by the user or failed renew the credentials.
 */
public final class UnAuthorizedException extends RuntimeException {
    public UnAuthorizedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public UnAuthorizedException(final String message) {
        super(message);
    }
}
