/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.exception;

public class MLBatchJobException extends RuntimeException {
    private int statusCode;
    public MLBatchJobException(String message, Throwable cause) {
        super(message, cause);
    }
    public MLBatchJobException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
