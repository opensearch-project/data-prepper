/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.exception;

import java.time.Duration;
import java.util.Optional;

/**
 * Exception class for source plugin exceptions.
 * Two following criterias:
 *  + Any REST API calls failures will be considered retryable
 *  + Other exceptions (e.g internal failures) will be considered non-retryable except writing to Buffer
 */
public class SaaSCrawlerException extends RuntimeException {
    private final boolean retryable;
    private final Duration retryDelay;

    public SaaSCrawlerException(String message, boolean retryable) {
        this(message, null, retryable, null);
    }

    public SaaSCrawlerException(String message, Throwable cause, boolean retryable) {
        this(message, cause, retryable, null);
    }

    public SaaSCrawlerException(String message, boolean retryable, Duration retryDelay) {
        this(message, null, retryable, retryDelay);
    }

    public SaaSCrawlerException(String message, Throwable cause, boolean retryable, Duration retryDelay) {
        super(message, cause);
        this.retryable = retryable;
        this.retryDelay = retryDelay;
    }

    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Returns a connector-selected delay before this partition is eligible for another acquisition.
     * An empty value preserves the scheduler's existing retry behavior.
     *
     * @return the partition retry delay, when one was requested
     */
    public Optional<Duration> getRetryDelay() {
        return Optional.ofNullable(retryDelay);
    }
}
