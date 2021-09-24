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
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.throttling.ThrottlingRejectHandler;

import javax.annotation.Nullable;

public class LogThrottlingRejectHandler implements ThrottlingRejectHandler<HttpRequest, HttpResponse> {
    private final int maxPendingRequests;

    public LogThrottlingRejectHandler(final int maxPendingRequests) {
        this.maxPendingRequests = maxPendingRequests;
    }

    @Override
    public HttpResponse handleRejected(final Service<HttpRequest, HttpResponse> delegate, final ServiceRequestContext ctx,
                                       final HttpRequest req, final @Nullable Throwable cause) throws Exception {
        return HttpResponse.of(HttpStatus.TOO_MANY_REQUESTS, MediaType.ANY_TYPE,
                "The number of pending requests in the work queue reaches max_pending_requests:%d. Please retry later",
                maxPendingRequests
        );
    }
}
