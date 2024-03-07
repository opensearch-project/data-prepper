/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.exception;

/**
 * Implementation class for EnrichFailedException Custom exception
 */
public class EnrichFailedException extends RuntimeException {
    public EnrichFailedException(final String exceptionMsg) {
        super(exceptionMsg);
    }
}
