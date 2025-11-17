/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry;

import lombok.Getter;

/**
 * Encapsulates the decision of whether to retry or stop
 */
@Getter
public class RetryDecision {
    private final boolean shouldStop;
    private final RuntimeException exception;

    private RetryDecision(boolean shouldStop, RuntimeException exception) {
        this.shouldStop = shouldStop;
        this.exception = exception;
    }

    public static RetryDecision retry() {
        return new RetryDecision(false, null);
    }

    public static RetryDecision stop() {
        return new RetryDecision(true, null);
    }

    public static RetryDecision stopWithException(RuntimeException exception) {
        return new RetryDecision(true, exception);
    }
}