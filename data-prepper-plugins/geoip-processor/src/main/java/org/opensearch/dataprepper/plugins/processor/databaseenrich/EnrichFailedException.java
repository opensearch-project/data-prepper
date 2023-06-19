/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

/**
 * Implementation class for EnrichFailedException Custom exception
 */
public class EnrichFailedException extends RuntimeException {
    public EnrichFailedException(String exceptionMsg) {
        super(exceptionMsg);
    }
}
