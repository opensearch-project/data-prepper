/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.exception;

public class MLBatchJobException extends RuntimeException {
    public MLBatchJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
