/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
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
import static org.opensearch.dataprepper.HttpRequestExceptionHandler.ARMERIA_REQUEST_TIMEOUT_MESSAGE;

@ExtendWith(MockitoExtension.class)
class HttpRequestExceptionHandlerTest {
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
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private HttpRequest httpRequest;

    private HttpRequestExceptionHandler httpRequestExceptionHandler;

    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);

        httpRequestExceptionHandler = new HttpRequestExceptionHandler(pluginMetrics);
    }

    @Test
    public void testHandleIOException() throws ExecutionException, InterruptedException {
        // Prepare
        final IOException testExceptionNullMessage = new IOException();
        final String testMessage = "test exception message";
        final IOException testExceptionWithMessage = new IOException(testMessage);

        // When
        HttpResponse httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionNullMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());
        assertEquals(HttpStatus.BAD_REQUEST.reasonPhrase(), aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // verify metrics
        verify(badRequestsCounter, times(2)).increment();
    }

    @Test
    public void testHandleTimeoutException() throws ExecutionException, InterruptedException {
        // Prepare
        final TimeoutException testExceptionNullMessage = new TimeoutException();
        final String testMessage = "test exception message";
        final TimeoutException testExceptionWithMessage = new TimeoutException(testMessage);

        // When
        HttpResponse httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionNullMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(HttpStatus.REQUEST_TIMEOUT.reasonPhrase(), aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // verify metrics
        verify(requestTimeoutsCounter, times(2)).increment();
    }

    @Test
    public void testHandleArmeriaTimeoutException() throws ExecutionException, InterruptedException {
        // Prepare
        final RequestTimeoutException testExceptionNullMessage = RequestTimeoutException.get();

        // When
        HttpResponse httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionNullMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(ARMERIA_REQUEST_TIMEOUT_MESSAGE, aggregatedHttpResponse.contentUtf8());

        // verify metrics
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleSizeOverflowException() throws ExecutionException, InterruptedException {
        // Prepare
        final SizeOverflowException testExceptionNoMessage = new SizeOverflowException(null);
        final String testMessage = "test exception message";
        final SizeOverflowException testExceptionWithMessage = new SizeOverflowException(testMessage);

        // When
        HttpResponse httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionNoMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE.reasonPhrase(), aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // verify metrics
        verify(requestsTooLargeCounter, times(2)).increment();
    }

    @Test
    public void testHandleUnknownException() throws ExecutionException, InterruptedException {
        // Prepare
        final UnknownException testExceptionNoMessage = new UnknownException(null);
        final String testMessage = "test exception message";
        final UnknownException testExceptionWithMessage = new UnknownException(testMessage);

        // When
        HttpResponse httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionNoMessage);

        // Then
        AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.reasonPhrase(), aggregatedHttpResponse.contentUtf8());

        // When
        httpResponse = httpRequestExceptionHandler.handleException(serviceRequestContext, httpRequest, testExceptionWithMessage);

        // Then
        aggregatedHttpResponse = httpResponse.aggregate().get();
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        // verify metrics
        verify(internalServerErrorCounter, times(2)).increment();
    }

    static class UnknownException extends Exception {
        public UnknownException(final String message) {
            super(message);
        }
    }
}