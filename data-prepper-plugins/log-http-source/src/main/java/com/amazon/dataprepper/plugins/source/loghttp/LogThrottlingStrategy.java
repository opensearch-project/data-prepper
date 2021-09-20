/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.UnmodifiableFuture;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.throttling.ThrottlingStrategy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;

public class LogThrottlingStrategy extends ThrottlingStrategy<HttpRequest> {
    private final int maxPendingRequests;
    private final BlockingQueue<Runnable> queue;

    public LogThrottlingStrategy(int maxPendingRequests, BlockingQueue<Runnable> queue) {
        this.maxPendingRequests = maxPendingRequests;
        this.queue = queue;
    }

    @Override
    public CompletionStage<Boolean> accept(ServiceRequestContext ctx, HttpRequest request) {
        // For ScheduledThreadPoolExecutor as blockingQueueExecutor, the DelayedWorkQueue is unbounded. Thus queue.size()
        // is used instead of queue.remainingCapacity().
        if (queue.size() <= maxPendingRequests) {
            return UnmodifiableFuture.completedFuture(true);
        }
        return UnmodifiableFuture.completedFuture(false);
    }
}
