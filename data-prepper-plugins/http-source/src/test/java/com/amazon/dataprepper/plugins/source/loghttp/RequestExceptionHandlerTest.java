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
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequestExceptionHandlerTest {
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

    private RequestExceptionHandler requestExceptionHandler;

    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(RequestExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(RequestExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(RequestExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(RequestExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);

        requestExceptionHandler = new RequestExceptionHandler(pluginMetrics);
    }

    @Test
    public void testHandleIOException() throws ExecutionException, InterruptedException {
        // Prepare
        final IOException testExceptionNullMessage = new IOException();
        final String testMessage = "test exception message";
        final IOException testExceptionWithMessage = new IOException(testMessage);

        // When
        HttpResponse httpResponse = requestExceptionHandler.handleException(testExceptionNullMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionNullMessage, testMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());
        // verify metrics
        verify(badRequestsCounter, times(3)).increment();
    }

    @Test
    public void testHandleTimeoutException() throws ExecutionException, InterruptedException {
        // Prepare
        final TimeoutException testExceptionNullMessage = new TimeoutException();
        final String testMessage = "test exception message";
        final TimeoutException testExceptionWithMessage = new TimeoutException(testMessage);

        // When
        HttpResponse httpResponse = requestExceptionHandler.handleException(testExceptionNullMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionNullMessage, testMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());
        // verify metrics
        verify(requestTimeoutsCounter, times(3)).increment();
    }

    @Test
    public void testHandleSizeOverflowException() throws ExecutionException, InterruptedException {
        // Prepare
        final SizeOverflowException testExceptionEmptyMessage = new SizeOverflowException("");
        final String testMessage = "test exception message";
        final SizeOverflowException testExceptionWithMessage = new SizeOverflowException(testMessage);

        // When
        HttpResponse httpResponse = requestExceptionHandler.handleException(testExceptionEmptyMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionEmptyMessage, testMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());
        // verify metrics
        verify(requestsTooLargeCounter, times(3)).increment();
    }

    @Test
    public void testHandleUnknownException() throws ExecutionException, InterruptedException {
        // Prepare
        final UnknownException testExceptionEmptyMessage = new UnknownException("");
        final String testMessage = "test exception message";
        final UnknownException testExceptionWithMessage = new UnknownException(testMessage);

        // When
        HttpResponse httpResponse = requestExceptionHandler.handleException(testExceptionEmptyMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = requestExceptionHandler.handleException(testExceptionEmptyMessage, testMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());
        // verify metrics
        verify(internalServerErrorCounter, times(3)).increment();
    }

    static class UnknownException extends Exception {
        public UnknownException(final String message) {
            super(message);
        }
    }
}