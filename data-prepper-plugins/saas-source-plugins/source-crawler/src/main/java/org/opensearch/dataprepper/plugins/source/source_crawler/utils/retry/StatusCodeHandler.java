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

/**
 * Handler for determining retry behavior based on HTTP status codes
 */
public interface StatusCodeHandler {
    /**
     * Handle an HTTP exception and determine whether to retry
     *
     * @param ex                The HTTP exception
     * @param retryCount        Current retry attempt
     * @param credentialRenewal Runnable to renew credentials
     * @return RetryDecision indicating whether to stop/continue and optional
     *         exception
     */
    RetryDecision handleStatusCode(Exception ex, int retryCount, Runnable credentialRenewal);
}