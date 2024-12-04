package org.opensearch.dataprepper.plugins.source.oteltrace.http;


import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.opensearch.dataprepper.GrpcRetryInfoCalculator;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.RetryInfo;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

// todo tlongo add metrics. See GrpcExceptionHandler
public class HttpExceptionHandler implements ExceptionHandlerFunction {
    private static final Logger LOG = LoggerFactory.getLogger(HttpExceptionHandler.class);

    static final String ARMERIA_REQUEST_TIMEOUT_MESSAGE = "Timeout waiting for request to be served. This is usually due to the buffer being full.";

    private final GrpcRetryInfoCalculator retryInfoCalculator;

    public HttpExceptionHandler(Duration retryInfoMinDelay, Duration retryInfoMaxDelay) {
        this.retryInfoCalculator = new GrpcRetryInfoCalculator(retryInfoMinDelay, retryInfoMaxDelay);
    }

    @Override
    public HttpResponse handleException(ServiceRequestContext ctx,
                                        HttpRequest req, Throwable e) {
        final Throwable exceptionCause = e instanceof BufferWriteException ? e.getCause() : e;
        StatusHolder statusHolder = createStatus(exceptionCause);

        try {
            // todo tlongo why do we need this in the first place?
            JsonFormat.TypeRegistry typeRegistry = JsonFormat.TypeRegistry.newBuilder()
                    .add(RetryInfo.getDescriptor())
                    .build();

            JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(typeRegistry);
            return HttpResponse.of(statusHolder.getHttpStatus(), MediaType.JSON, printer.print(statusHolder.getStatus()));
        } catch (InvalidProtocolBufferException ipbe) {
            throw new RuntimeException(ipbe);
        }
    }

    private StatusHolder createStatus(Throwable e) {
        if (e instanceof RequestTimeoutException || e instanceof TimeoutException) {
            return new StatusHolder(createStatus(e, Status.Code.RESOURCE_EXHAUSTED), createHttpStatusFromProtoBufStatus(Status.Code.RESOURCE_EXHAUSTED));
        } else if (e instanceof SizeOverflowException) {
            return new StatusHolder(createStatus(e, Status.Code.RESOURCE_EXHAUSTED), createHttpStatusFromProtoBufStatus(Status.Code.RESOURCE_EXHAUSTED));
        } else if (e instanceof BadRequestException) {
            return new StatusHolder(createStatus(e, Status.Code.INVALID_ARGUMENT), createHttpStatusFromProtoBufStatus(Status.Code.INVALID_ARGUMENT));
        } else if ((e instanceof StatusRuntimeException) && (e.getMessage().contains("Invalid protobuf byte sequence") || e.getMessage().contains("Can't decode compressed frame"))) {
            return new StatusHolder(createStatus(e, Status.Code.INVALID_ARGUMENT), createHttpStatusFromProtoBufStatus(Status.Code.INVALID_ARGUMENT));
        } else if (e instanceof RequestCancelledException) {
            return new StatusHolder(createStatus(e, Status.Code.CANCELLED), createHttpStatusFromProtoBufStatus(Status.Code.CANCELLED));
        } else {
            LOG.error("Unexpected exception handling http request", e);
            return new StatusHolder(createStatus(e, Status.Code.INTERNAL), createHttpStatusFromProtoBufStatus(Status.Code.INTERNAL));
        }
    }

    private HttpStatus createHttpStatusFromProtoBufStatus(Status.Code status) {
        if (status == Status.Code.RESOURCE_EXHAUSTED) {
            return HttpStatus.INSUFFICIENT_STORAGE;
        } else if (status == Status.Code.INVALID_ARGUMENT) {
            return HttpStatus.BAD_REQUEST;
        } else {
            return HttpStatus.INTERNAL_SERVER_ERROR;
        }
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

    private static class StatusHolder {
        private final HttpStatus httpStatus;
        private final com.google.rpc.Status status;

        public StatusHolder(com.google.rpc.Status status, HttpStatus httpStatus) {
            this.httpStatus = httpStatus;
            this.status = status;
        }

        public HttpStatus getHttpStatus() {
            return httpStatus;
        }

        public com.google.rpc.Status getStatus() {
            return status;
        }
    }

}
