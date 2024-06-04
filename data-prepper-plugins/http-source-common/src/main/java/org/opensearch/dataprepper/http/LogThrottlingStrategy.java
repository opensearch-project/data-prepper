/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.UnmodifiableFuture;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.throttling.ThrottlingStrategy;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;

public class LogThrottlingStrategy extends ThrottlingStrategy<HttpRequest> {
    private final int maxPendingRequests;
    private final BlockingQueue<Runnable> queue;

    public LogThrottlingStrategy(final int maxPendingRequests, final BlockingQueue<Runnable> queue) {
        Objects.requireNonNull(queue);
        this.maxPendingRequests = maxPendingRequests;
        this.queue = queue;
    }

    @Override
    public CompletionStage<Boolean> accept(ServiceRequestContext ctx, HttpRequest request) {
        /*
         * TODO:
         * The current implementation based on the condition queue.size() < maxPendingRequests is loose, i.e.
         * in case of high concurrency, multiple thread could pass this check concurrently and push their tasks into
         * the queue. Thus the actual queue size could still exceed maxPendingRequests in high concurrency.
         * There is no way to strictly deal with this issue at the ThrottlingStrategy level. Instead,
         * we need to deal with it at the blockingExecutor level. Currently, the blockingExecutor of armeria server only accepts
         * {@link java.util.concurrent.ScheduledExecutorService} which uses unbounded {@link java.util.concurrent.ScheduledThreadPoolExecutor.DelayedWorkQueue}.
         * The potential workaround is in the discussion of https://github.com/line/armeria/issues/2694.
         */
        if (queue.size() < maxPendingRequests) {
            return UnmodifiableFuture.completedFuture(true);
        }
        return UnmodifiableFuture.completedFuture(false);
    }
}
