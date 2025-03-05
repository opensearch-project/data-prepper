/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.exception;

/**
 * Exception to indicate failed to get metadata from SQL database
 */
public class SqlMetadataException extends RuntimeException {
    public SqlMetadataException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public SqlMetadataException(final String message) {
        super(message);
    }
}
