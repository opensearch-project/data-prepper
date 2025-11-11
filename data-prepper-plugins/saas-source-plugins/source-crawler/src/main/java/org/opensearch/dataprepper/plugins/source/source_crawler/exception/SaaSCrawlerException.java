/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.exception;

/**
 * Exception class for source plugin exceptions.
 * Two following criterias:
 *  + Any REST API calls failures will be considered retryable
 *  + Other exceptions (e.g internal failures) will be considered non-retryable except writing to Buffer
 */
public class SaaSCrawlerException extends RuntimeException {
    private final boolean retryable;

    public SaaSCrawlerException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public SaaSCrawlerException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
