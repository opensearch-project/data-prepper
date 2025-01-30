/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.exception;

/**
 * Exception to indicate a bad REST call has been made.
 * It could either be caused by bad user inputs or wrong url construction in the logic.
 */
public final class BadRequestException extends RuntimeException {
    public BadRequestException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public BadRequestException(final String message) {
        super(message);
    }
}
