/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.server.ResponseHandler;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.core.peerforwarder.server.ResponseHandler.BAD_REQUESTS;
import static org.opensearch.dataprepper.core.peerforwarder.server.ResponseHandler.REQUESTS_TOO_LARGE;
import static org.opensearch.dataprepper.core.peerforwarder.server.ResponseHandler.REQUESTS_UNPROCESSABLE;
import static org.opensearch.dataprepper.core.peerforwarder.server.ResponseHandler.REQUEST_TIMEOUTS;

@ExtendWith(MockitoExtension.class)
class ResponseHandlerTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter requestsTooLargeCounter;

    @Mock
    private Counter requestTimeoutsCounter;

    @Mock
    private Counter badRequestsCounter;

    @Mock
    private Counter requestsUnprocessableCounter;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(REQUESTS_UNPROCESSABLE)).thenReturn(requestsUnprocessableCounter);
        when(pluginMetrics.counter(BAD_REQUESTS)).thenReturn(badRequestsCounter);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(requestsTooLargeCounter, requestTimeoutsCounter, requestsUnprocessableCounter, badRequestsCounter);
    }

    private ResponseHandler createObjectUnderTest() {
        return new ResponseHandler(pluginMetrics);
    }

    @Test
    void test_JsonProcessingException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final JsonProcessingException jsonProcessingException = mock(JsonProcessingException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(jsonProcessingException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        verify(badRequestsCounter).increment();
    }

    @Test
    void test_SizeOverflowException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final SizeOverflowException sizeOverflowException = mock(SizeOverflowException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(sizeOverflowException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        verify(requestsTooLargeCounter).increment();
    }

    @Test
    void test_TimeoutException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final TimeoutException timeoutException = mock(TimeoutException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(timeoutException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        verify(requestTimeoutsCounter).increment();
    }

    @Test
    void test_UnknownException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final UnknownException unknownException = new UnknownException("");

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(unknownException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.BAD_REQUEST, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        verify(badRequestsCounter).increment();
    }

    @Test
    void test_NullPointerException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final NullPointerException nullPointerException = new NullPointerException("");

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(nullPointerException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        verify(requestsUnprocessableCounter).increment();
    }

    static class UnknownException extends Exception {
        public UnknownException(final String message) {
            super(message);
        }
    }

}