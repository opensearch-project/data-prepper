/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common.client;

import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamResponseHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

/**
 * Wraps a {@link LambdaAsyncClient} with a {@link Semaphore} sized to the HTTP client's
 * maximum connection pool size. A permit is held for the full duration of each API call,
 * so the number of in-flight calls can never exceed the connection pool size. Callers
 * block on permit acquisition, which propagates backpressure to pipeline worker threads
 * instead of exhausting the connection pool and failing requests.
 *
 * <p>SDK-level retry attempts within a single API call are sequential and hold at most
 * one pooled connection at a time, so one permit per API call matches the pool exactly.
 */
public class BoundedLambdaAsyncClient {
    private final LambdaAsyncClient delegate;
    private final int maxConcurrency;
    private final Semaphore concurrencyPermits;

    public BoundedLambdaAsyncClient(final LambdaAsyncClient delegate, final int maxConcurrency) {
        this.delegate = delegate;
        this.maxConcurrency = maxConcurrency;
        this.concurrencyPermits = new Semaphore(maxConcurrency);
    }

    public CompletableFuture<InvokeResponse> invoke(final InvokeRequest invokeRequest) {
        acquirePermit();
        try {
            return delegate.invoke(invokeRequest)
                    .whenComplete((response, throwable) -> concurrencyPermits.release());
        } catch (final RuntimeException e) {
            concurrencyPermits.release();
            throw e;
        }
    }

    public CompletableFuture<Void> invokeWithResponseStream(
            final InvokeWithResponseStreamRequest request,
            final InvokeWithResponseStreamResponseHandler responseHandler) {
        acquirePermit();
        try {
            return delegate.invokeWithResponseStream(request, responseHandler)
                    .whenComplete((response, throwable) -> concurrencyPermits.release());
        } catch (final RuntimeException e) {
            concurrencyPermits.release();
            throw e;
        }
    }

    /**
     * Returns the number of API calls which may be started without blocking. This value
     * is also the headroom remaining in the underlying HTTP connection pool.
     *
     * @return the number of available permits
     */
    public int getAvailableConcurrency() {
        return concurrencyPermits.availablePermits();
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public LambdaAsyncClient getDelegate() {
        return delegate;
    }

    private void acquirePermit() {
        try {
            concurrencyPermits.acquire();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a Lambda connection permit", e);
        }
    }
}
