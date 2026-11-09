/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common;

/**
 * Thrown when a streamed Lambda invocation ends with a function error, which Lambda reports as an error code on the
 * InvokeComplete event rather than as an exception.
 */
public final class StreamingLambdaInvocationException extends RuntimeException {

    private final String errorCode;

    public StreamingLambdaInvocationException(final String errorCode, final String errorDetails) {
        super(errorDetails == null ? errorCode : errorCode + ": " + errorDetails);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
