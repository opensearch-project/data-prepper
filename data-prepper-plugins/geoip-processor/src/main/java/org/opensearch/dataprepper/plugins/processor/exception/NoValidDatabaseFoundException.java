/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.exception;

public class NoValidDatabaseFoundException extends RuntimeException {
    public NoValidDatabaseFoundException(final String exceptionMsg) {
        super(exceptionMsg);
    }
}
