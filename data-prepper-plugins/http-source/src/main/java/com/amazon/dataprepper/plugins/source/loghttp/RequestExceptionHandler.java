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

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.SizeOverflowException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import io.micrometer.core.instrument.Counter;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class RequestExceptionHandler {
    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";

    private final Counter requestTimeoutsCounter;
    private final Counter badRequestsCounter;
    private final Counter requestsTooLargeCounter;

    public RequestExceptionHandler(final PluginMetrics pluginMetrics) {
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
    }

    public HttpResponse handleException(final Exception e) {
        final String message = e.getMessage() == null? "" : e.getMessage();
        return handleException(e, message);
    }

    public HttpResponse handleException(final Exception e, final String message) {
        Objects.requireNonNull(message);
        if (e instanceof IOException) {
            badRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, message);
        } else if (e instanceof TimeoutException) {
            requestTimeoutsCounter.increment();
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT, MediaType.ANY_TYPE, message);
        } else if (e instanceof SizeOverflowException) {
            requestsTooLargeCounter.increment();
            return HttpResponse.of(HttpStatus.REQUEST_ENTITY_TOO_LARGE, MediaType.ANY_TYPE, message);
        } else {
            return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR, MediaType.ANY_TYPE, message);
        }
    }
}
