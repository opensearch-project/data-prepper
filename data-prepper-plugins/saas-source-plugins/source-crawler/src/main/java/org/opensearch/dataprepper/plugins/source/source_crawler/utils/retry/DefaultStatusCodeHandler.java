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

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Default status code handling - covers common HTTP scenarios
 */
@Slf4j
public class DefaultStatusCodeHandler implements StatusCodeHandler {

    @Override
    public RetryDecision handleStatusCode(Exception ex, int retryCount,
                                          Runnable credentialRenewal) {
        Optional<HttpStatus> statusCode = RetryStrategy.getStatusCode(ex);
        String statusMessage = ex.getMessage();

        if (statusCode.isEmpty()) {
            return RetryDecision.stop();
        }

        switch (statusCode.get()) {
            case UNAUTHORIZED:
                log.error(NOISY, "Token expired. Attempting to renew credentials.", ex);
                credentialRenewal.run();
                return RetryDecision.retry();

            case FORBIDDEN:
                log.error(NOISY, "Access forbidden: {}", statusMessage, ex);
                return RetryDecision.stopWithException(
                        new SecurityException("Access forbidden: " + statusMessage));

            case NOT_FOUND:
                log.warn(NOISY, "Resource not found (404): {}. " +
                        "This is expected for deleted/expired resources.", statusMessage);
                return RetryDecision.stop();

            case TOO_MANY_REQUESTS:
                log.error(NOISY, "Hitting API rate limit. Backing off.", ex);
                return RetryDecision.retry();

            case SERVICE_UNAVAILABLE:
                log.error(NOISY, "Service unavailable. Will retry.", ex);
                return RetryDecision.retry();

            default:
                if (statusCode.get().is4xxClientError()) {
                    log.error(NOISY, "Client error: {}. Will not retry.", statusCode, ex);
                    return RetryDecision.stop();
                } else if (statusCode.get().is5xxServerError()) {
                    log.error(NOISY, "Server error: {}. Will retry.", statusCode, ex);
                    return RetryDecision.retry();
                } else {
                    log.error(NOISY, "Unexpected status code: {}. Will not retry.",
                            statusCode, ex);
                    return RetryDecision.stop();
                }
        }
    }
}
