/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import io.micrometer.core.instrument.Counter;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class HttpRequestExceptionHandler implements ExceptionHandlerFunction {
    static final String ARMERIA_REQUEST_TIMEOUT_MESSAGE = "Timeout waiting for request to be served. This is usually due to the buffer being full.";

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "internalServerError";

    private final Counter requestTimeoutsCounter;
    private final Counter badRequestsCounter;
    private final Counter requestsTooLargeCounter;
    private final Counter internalServerErrorCounter;

    public HttpRequestExceptionHandler(final PluginMetrics pluginMetrics) {
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
        internalServerErrorCounter = pluginMetrics.counter(INTERNAL_SERVER_ERROR);
    }

    @Override
    public HttpResponse handleException(final ServiceRequestContext ctx, final HttpRequest req, final Throwable cause) {
        final HttpStatus status = handleException(cause);
        final String message;
        if (cause instanceof RequestTimeoutException) {
            message = ARMERIA_REQUEST_TIMEOUT_MESSAGE;
        } else {
            message = cause.getMessage() == null ? status.reasonPhrase() : cause.getMessage();
        }

        return HttpResponse.of(status, MediaType.ANY_TYPE, message);
    }

    private HttpStatus handleException(final Throwable e) {
        if (e instanceof IOException) {
            badRequestsCounter.increment();
            return HttpStatus.BAD_REQUEST;
        } else if (e instanceof TimeoutException || e instanceof RequestTimeoutException) {
            requestTimeoutsCounter.increment();
            return HttpStatus.REQUEST_TIMEOUT;
        } else if (e instanceof SizeOverflowException) {
            requestsTooLargeCounter.increment();
            return HttpStatus.REQUEST_ENTITY_TOO_LARGE;
        }

        internalServerErrorCounter.increment();
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }
}
