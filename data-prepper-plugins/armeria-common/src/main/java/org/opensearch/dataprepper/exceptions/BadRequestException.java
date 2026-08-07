/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.exceptions;

public class BadRequestException extends RuntimeException {
    public BadRequestException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
