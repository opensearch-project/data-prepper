/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.google.protobuf.Any;
import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.common.grpc.GoogleGrpcExceptionHandlerFunction;
import com.linecorp.armeria.server.RequestTimeoutException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Counter;

import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class GrpcRequestExceptionHandler implements GoogleGrpcExceptionHandlerFunction {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcRequestExceptionHandler.class);
    static final String ARMERIA_REQUEST_TIMEOUT_MESSAGE = "Timeout waiting for request to be served. This is usually due to the buffer being full.";

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "internalServerError";

    private final Counter requestTimeoutsCounter;
    private final Counter badRequestsCounter;
    private final Counter requestsTooLargeCounter;
    private final Counter internalServerErrorCounter;
    private final GrpcRetryInfoCalculator retryInfoCalculator;

    public GrpcRequestExceptionHandler(final PluginMetrics pluginMetrics) {
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
        internalServerErrorCounter = pluginMetrics.counter(INTERNAL_SERVER_ERROR);
        retryInfoCalculator = new GrpcRetryInfoCalculator(Duration.ofMillis(100), Duration.ofSeconds(2));
    }

    @Override
    public com.google.rpc.@Nullable Status applyStatusProto(RequestContext ctx, Throwable throwable,
                                                            Metadata metadata) {
        final Throwable exceptionCause = throwable instanceof BufferWriteException ? throwable.getCause() : throwable;
        return handleExceptions(exceptionCause);
    }

    private com.google.rpc.Status handleExceptions(final Throwable e) {
        String message = e.getMessage();
        if (e instanceof RequestTimeoutException || e instanceof TimeoutException) {
            requestTimeoutsCounter.increment();
            return createStatus(e, Status.Code.RESOURCE_EXHAUSTED);
        } else if (e instanceof SizeOverflowException) {
            requestsTooLargeCounter.increment();
            return createStatus(e, Status.Code.RESOURCE_EXHAUSTED);
        } else if (e instanceof BadRequestException) {
            badRequestsCounter.increment();
            return createStatus(e, Status.Code.INVALID_ARGUMENT);
        } else if ((e instanceof StatusRuntimeException) && (message.contains("Invalid protobuf byte sequence") || message.contains("Can't decode compressed frame"))) {
            badRequestsCounter.increment();
            return createStatus(e, Status.Code.INVALID_ARGUMENT);
        } else if (e instanceof RequestCancelledException) {
            requestTimeoutsCounter.increment();
            return createStatus(e, Status.Code.CANCELLED);
        }

        internalServerErrorCounter.increment();
        LOG.error("Unexpected exception handling gRPC request", e);
        return createStatus(e, Status.Code.INTERNAL);
    }

    private com.google.rpc.Status createStatus(final Throwable e, final Status.Code code) {
        com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder().setCode(code.value());
        if (e instanceof RequestTimeoutException) {
            builder.setMessage(ARMERIA_REQUEST_TIMEOUT_MESSAGE);
        } else {
            builder.setMessage(e.getMessage() == null ? code.name() :e.getMessage());
        }
        if (code == Status.Code.RESOURCE_EXHAUSTED) {
            builder.addDetails(Any.pack(retryInfoCalculator.createRetryInfo()));
        }
        return builder.build();
    }
}
