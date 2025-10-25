/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

@Slf4j
public class RetryHandler {
    public static final List<Integer> RETRY_ATTEMPT_SLEEP_TIME = List.of(1, 2, 5, 10, 20, 40);
    private static final int MAX_RETRIES = 6;
    private static final int SLEEP_TIME_MULTIPLIER = 1000;

    public static <T> T executeWithRetry(Supplier<T> operation, Runnable credentialRenewal) {
        return executeWithRetry(operation, credentialRenewal, null);
    }

    public static <T> T executeWithRetry(Supplier<T> operation, Runnable credentialRenewal, Counter failureCounter) {
        int retryCount = 0;
        while (retryCount < MAX_RETRIES) {
            boolean operationSucceeded = false;
            try {
                T result = operation.get();
                operationSucceeded = true;
                return result;
            } catch (HttpClientErrorException | HttpServerErrorException ex) {
                HttpStatus statusCode = ex.getStatusCode();
                String statusMessage = ex.getMessage();

                switch (statusCode) {
                    case UNAUTHORIZED:
                        log.error(NOISY, "Token expired. Attempting to renew credentials.", ex);
                        credentialRenewal.run();
                        break;
                    case FORBIDDEN:
                        log.error(NOISY, "Access forbidden: {}", statusMessage, ex);
                        throw new SecurityException("Access forbidden: " + statusMessage);
                    case TOO_MANY_REQUESTS:
                        log.error(NOISY, "Hitting API rate limit. Backing off with sleep timer.", ex);
                        break;
                    case SERVICE_UNAVAILABLE:
                        log.error(NOISY, "Service is unavailable. Will retry after backing off.", ex);
                        break;
                    default:
                        if (ex.getStatusCode().is4xxClientError()) {
                            log.error(NOISY, "Client error: {}. Will not retry.", statusCode, ex);
                            throw ex;
                        } else if (ex.getStatusCode().is5xxServerError()) {
                            log.error(NOISY, "Server error: {}. Will retry after backing off.", statusCode, ex);
                        } else {
                            throw ex;
                        }
                }

                if (retryCount == MAX_RETRIES - 1) {
                    log.error(NOISY, "Exceeded maximum retry attempts.", ex);
                    throw ex;
                }

                try {
                    Thread.sleep((long) RETRY_ATTEMPT_SLEEP_TIME.get(retryCount) * SLEEP_TIME_MULTIPLIER);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            } finally {
                if (!operationSucceeded && failureCounter != null) {
                    failureCounter.increment();
                }
            }
            retryCount++;
        }
        throw new RuntimeException("Exceeded max retry attempts");
    }
}
