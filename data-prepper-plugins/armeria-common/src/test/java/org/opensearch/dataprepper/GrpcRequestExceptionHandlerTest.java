/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.server.RequestTimeoutException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.GrpcRequestExceptionHandler.ARMERIA_REQUEST_TIMEOUT_MESSAGE;

@ExtendWith(MockitoExtension.class)
public class GrpcRequestExceptionHandlerTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter requestTimeoutsCounter;

    @Mock
    private Counter badRequestsCounter;

    @Mock
    private Counter requestsTooLargeCounter;

    @Mock
    private Counter internalServerErrorCounter;

    @Mock
    private RequestContext requestContext;

    @Mock
    private Metadata metadata;

    private GrpcRequestExceptionHandler grpcRequestExceptionHandler;

    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);

        grpcRequestExceptionHandler = new GrpcRequestExceptionHandler(pluginMetrics);
    }

    @Test
    public void testHandleBadRequestException() {
        final BadRequestException badRequestExceptionNoMessage = new BadRequestException(null, new IOException());
        final String exceptionMessage = UUID.randomUUID().toString();
        final BadRequestException badRequestExceptionWithMessage = new BadRequestException(exceptionMessage, new IOException());

        final Status noMessageStatus = grpcRequestExceptionHandler.apply(requestContext, badRequestExceptionNoMessage, metadata);
        assertThat(noMessageStatus.getCode(), equalTo(Status.Code.INVALID_ARGUMENT));
        assertThat(noMessageStatus.getDescription(), equalTo(Status.Code.INVALID_ARGUMENT.name()));

        final Status messageStatus = grpcRequestExceptionHandler.apply(requestContext, badRequestExceptionWithMessage, metadata);
        assertThat(messageStatus.getCode(), equalTo(Status.Code.INVALID_ARGUMENT));
        assertThat(messageStatus.getDescription(), equalTo(exceptionMessage));

        verify(badRequestsCounter, times(2)).increment();
    }

    @Test
    public void testHandleTimeoutException() {
        final BufferWriteException timeoutExceptionNoMessage = new BufferWriteException(null, new TimeoutException());
        final String exceptionMessage = UUID.randomUUID().toString();
        final BufferWriteException timeoutExceptionWithMessage = new BufferWriteException(exceptionMessage, new TimeoutException(exceptionMessage));

        final Status noMessageStatus = grpcRequestExceptionHandler.apply(requestContext, timeoutExceptionNoMessage, metadata);
        assertThat(noMessageStatus.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(noMessageStatus.getDescription(), equalTo(Status.Code.RESOURCE_EXHAUSTED.name()));

        final Status messageStatus = grpcRequestExceptionHandler.apply(requestContext, timeoutExceptionWithMessage, metadata);
        assertThat(messageStatus.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(messageStatus.getDescription(), equalTo(exceptionMessage));

        verify(requestTimeoutsCounter, times(2)).increment();
    }

    @Test
    public void testHandleArmeriaTimeoutException() {
        final RequestTimeoutException timeoutExceptionNoMessage = RequestTimeoutException.get();

        final Status noMessageStatus = grpcRequestExceptionHandler.apply(requestContext, timeoutExceptionNoMessage, metadata);
        assertThat(noMessageStatus.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(noMessageStatus.getDescription(), equalTo(ARMERIA_REQUEST_TIMEOUT_MESSAGE));

        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleSizeOverflowException() {
        final BufferWriteException sizeOverflowExceptionNoMessage = new BufferWriteException(null, new SizeOverflowException(null));
        final String exceptionMessage = UUID.randomUUID().toString();
        final BufferWriteException sizeOverflowExceptionWithMessage = new BufferWriteException(exceptionMessage, new SizeOverflowException(exceptionMessage));

        final Status noMessageStatus = grpcRequestExceptionHandler.apply(requestContext, sizeOverflowExceptionNoMessage, metadata);
        assertThat(noMessageStatus.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(noMessageStatus.getDescription(), equalTo(Status.Code.RESOURCE_EXHAUSTED.name()));

        final Status messageStatus = grpcRequestExceptionHandler.apply(requestContext, sizeOverflowExceptionWithMessage, metadata);
        assertThat(messageStatus.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(messageStatus.getDescription(), equalTo(exceptionMessage));

        verify(requestsTooLargeCounter, times(2)).increment();
    }

    @Test
    public void testHandleRequestCancelledException() {
        final RequestCancelledException requestCancelledExceptionNoMessage = new RequestCancelledException(null);
        final String exceptionMessage = UUID.randomUUID().toString();
        final RequestCancelledException requestCancelledExceptionWithMessage = new RequestCancelledException(exceptionMessage);

        final Status noMessageStatus = grpcRequestExceptionHandler.apply(requestContext, requestCancelledExceptionNoMessage, metadata);
        assertThat(noMessageStatus.getCode(), equalTo(Status.Code.CANCELLED));
        assertThat(noMessageStatus.getDescription(), equalTo(Status.Code.CANCELLED.name()));

        final Status messageStatus = grpcRequestExceptionHandler.apply(requestContext, requestCancelledExceptionWithMessage, metadata);
        assertThat(messageStatus.getCode(), equalTo(Status.Code.CANCELLED));
        assertThat(messageStatus.getDescription(), equalTo(exceptionMessage));

        verify(requestTimeoutsCounter, times(2)).increment();
    }

    @Test
    public void testHandleInternalServerException() {
        final RuntimeException runtimeExceptionNoMessage = new RuntimeException();
        final String exceptionMessage = UUID.randomUUID().toString();
        final RuntimeException runtimeExceptionWithMessage = new RuntimeException(exceptionMessage);

        final Status noMessageStatus = grpcRequestExceptionHandler.apply(requestContext, runtimeExceptionNoMessage, metadata);
        assertThat(noMessageStatus.getCode(), equalTo(Status.Code.INTERNAL));
        assertThat(noMessageStatus.getDescription(), equalTo(Status.Code.INTERNAL.name()));

        final Status messageStatus = grpcRequestExceptionHandler.apply(requestContext, runtimeExceptionWithMessage, metadata);
        assertThat(messageStatus.getCode(), equalTo(Status.Code.INTERNAL));
        assertThat(messageStatus.getDescription(), equalTo(exceptionMessage));

        verify(internalServerErrorCounter, times(2)).increment();
    }
}
