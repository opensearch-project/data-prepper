/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.exception;

/**
 * Exception to indicate unauthorized access.
 * It could either be caused by invalid credentials supplied by the user or failed renew the credentials.
 */
public final class UnauthorizedException extends RuntimeException {
    public UnauthorizedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public UnauthorizedException(final String message) {
        super(message);
    }
}
