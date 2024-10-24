/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Class to handle exceptions while processing HTTP POST requests by {@link PeerForwarderHttpService}
 * and sends back the HTTP response based on exception
 * @since 2.0
 */
public class ResponseHandler {
    static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    static final String REQUEST_TIMEOUTS = "requestTimeouts";
    static final String REQUESTS_UNPROCESSABLE = "requestsUnprocessable";
    static final String BAD_REQUESTS = "badRequests";

    private final Counter requestsTooLargeCounter;
    private final Counter requestTimeoutsCounter;
    private final Counter requestsUnprocessableCounter;
    private final Counter badRequestsCounter;

    public ResponseHandler(final PluginMetrics pluginMetrics) {
        requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        requestsUnprocessableCounter = pluginMetrics.counter(REQUESTS_UNPROCESSABLE);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
    }

    public HttpResponse handleException(final Exception e, final String message) {
        Objects.requireNonNull(message);

        if (e instanceof SizeOverflowException) {
            requestsTooLargeCounter.increment();
            return HttpResponse.of(HttpStatus.REQUEST_ENTITY_TOO_LARGE, MediaType.ANY_TYPE, message);
        }

        if (e instanceof TimeoutException) {
            requestTimeoutsCounter.increment();
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT, MediaType.ANY_TYPE, message);
        }

        if (e instanceof NullPointerException) {
            requestsUnprocessableCounter.increment();
            return HttpResponse.of(HttpStatus.UNPROCESSABLE_ENTITY, MediaType.ANY_TYPE, message);
        }

        badRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, message);
    }
}
