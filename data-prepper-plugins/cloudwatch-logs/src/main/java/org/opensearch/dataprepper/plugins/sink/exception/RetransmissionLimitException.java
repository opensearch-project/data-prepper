/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.exception;

public class RetransmissionLimitException extends RuntimeException{
    public RetransmissionLimitException(String message) {
        super(message);
    }
}