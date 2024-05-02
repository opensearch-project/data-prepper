/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.throttling.ThrottlingRejectHandler;
import io.micrometer.core.instrument.Counter;

import javax.annotation.Nullable;

public class LogThrottlingRejectHandler implements ThrottlingRejectHandler<HttpRequest, HttpResponse> {
    public static final String REQUESTS_REJECTED = "requestsRejected";

    private final int maxPendingRequests;
    private final Counter rejectedRequestsCounter;

    public LogThrottlingRejectHandler(final int maxPendingRequests, final PluginMetrics pluginMetrics) {
        this.maxPendingRequests = maxPendingRequests;

        rejectedRequestsCounter = pluginMetrics.counter(REQUESTS_REJECTED);
    }

    @Override
    public HttpResponse handleRejected(final Service<HttpRequest, HttpResponse> delegate, final ServiceRequestContext ctx,
                                       final HttpRequest req, final @Nullable Throwable cause) throws Exception {
        rejectedRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.TOO_MANY_REQUESTS, MediaType.ANY_TYPE,
                "The number of pending requests in the work queue reaches max_pending_requests:%d. Please retry later",
                maxPendingRequests
        );
    }
}
