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

public class CustomAuthenticationExceptionHandler implements GoogleGrpcExceptionHandlerFunction {
    private static final Logger LOG = LoggerFactory.getLogger(CustomAuthenticationExceptionHandler.class);
    private static final String TIMEOUT_MESSAGE = "Request timed out. Check buffer availability or processing delays.";

    public static final String REQUEST_TIMEOUTS = "customAuthRequestTimeouts";
    public static final String BAD_REQUESTS = "customAuthBadRequests";
    public static final String REQUESTS_TOO_LARGE = "customAuthRequestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "customAuthInternalServerError";

    private final Counter requestTimeoutsCounter;
    private final Counter badRequestsCounter;
    private final Counter requestsTooLargeCounter;
    private final Counter internalServerErrorCounter;
    private final GrpcRetryInfoCalculator retryInfoCalculator;

    public CustomAuthenticationExceptionHandler(final PluginMetrics pluginMetrics,
                                                final Duration retryInfoMinDelay,
                                                final Duration retryInfoMaxDelay) {
        this.requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        this.badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        this.requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
        this.internalServerErrorCounter = pluginMetrics.counter(INTERNAL_SERVER_ERROR);
        this.retryInfoCalculator = new GrpcRetryInfoCalculator(retryInfoMinDelay, retryInfoMaxDelay);
    }

    @Override
    public com.google.rpc.@Nullable Status applyStatusProto(RequestContext ctx, Throwable throwable, Metadata metadata) {
        final Throwable actualCause = (throwable instanceof BufferWriteException)
                ? throwable.getCause() : throwable;
        return handleException(actualCause);
    }

    private com.google.rpc.Status handleException(Throwable e) {
        final String msg = e.getMessage();
        if (e instanceof RequestTimeoutException || e instanceof TimeoutException) {
            requestTimeoutsCounter.increment();
            return buildStatus(e, Status.Code.RESOURCE_EXHAUSTED);
        } else if (e instanceof SizeOverflowException) {
            requestsTooLargeCounter.increment();
            return buildStatus(e, Status.Code.RESOURCE_EXHAUSTED);
        } else if (e instanceof BadRequestException) {
            badRequestsCounter.increment();
            return buildStatus(e, Status.Code.INVALID_ARGUMENT);
        } else if ((e instanceof StatusRuntimeException) &&
                (msg.contains("Invalid protobuf byte sequence") || msg.contains("Can't decode compressed frame"))) {
            badRequestsCounter.increment();
            return buildStatus(e, Status.Code.INVALID_ARGUMENT);
        } else if (e instanceof RequestCancelledException) {
            requestTimeoutsCounter.increment();
            return buildStatus(e, Status.Code.CANCELLED);
        }

        internalServerErrorCounter.increment();
        LOG.error("CustomAuth gRPC handler caught unexpected exception", e);
        return buildStatus(e, Status.Code.INTERNAL);
    }

    private com.google.rpc.Status buildStatus(Throwable e, Status.Code code) {
        com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder()
                .setCode(code.value());

        if (e instanceof RequestTimeoutException) {
            builder.setMessage(TIMEOUT_MESSAGE);
        } else {
            builder.setMessage(e.getMessage() != null ? e.getMessage() : code.name());
        }

        if (code == Status.Code.RESOURCE_EXHAUSTED) {
            builder.addDetails(Any.pack(retryInfoCalculator.createRetryInfo()));
        }

        return builder.build();
    }
}

