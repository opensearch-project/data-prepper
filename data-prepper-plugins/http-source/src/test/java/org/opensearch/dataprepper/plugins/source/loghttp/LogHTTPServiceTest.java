/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogHTTPServiceTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int TEST_BUFFER_CAPACITY = 3;
    private static final int TEST_TIMEOUT_IN_MILLIS = 500;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter requestsReceivedCounter;

    @Mock
    private Counter requestTimeoutsCounter;

    @Mock
    private Counter successRequestsCounter;

    @Mock
    private Counter badRequestsCounter;

    @Mock
    private Counter requestsTooLargeCounter;

    @Mock
    private Counter internalServerErrorCounter;

    @Mock
    private DistributionSummary payloadSizeSummary;

    @Mock
    private Timer requestProcessDuration;

    private LogHTTPService logHTTPService;

    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(LogHTTPService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(LogHTTPService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);
        when(pluginMetrics.summary(LogHTTPService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(pluginMetrics.timer(LogHTTPService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        when(requestProcessDuration.record(ArgumentMatchers.<Supplier<HttpResponse>>any())).thenAnswer(
                (Answer<HttpResponse>) invocation -> {
                    final Object[] args = invocation.getArguments();
                    @SuppressWarnings("unchecked")
                    final Supplier<HttpResponse> supplier = (Supplier<HttpResponse>) args[0];
                    return supplier.get();
                }
        );

        Buffer<Record<Log>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_CAPACITY, 8, "test-pipeline");
        logHTTPService = new LogHTTPService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
    }

    @Test
    public void testHTTPRequestSuccess() throws InterruptedException, ExecutionException, JsonProcessingException {
        // Prepare
        AggregatedHttpRequest testRequest = generateRandomValidHTTPRequest(2);

        // When
        AggregatedHttpResponse postResponse = logHTTPService.doPost(testRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.OK, postResponse.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(requestTimeoutsCounter, never()).increment();
        verify(successRequestsCounter, times(1)).increment();
        verify(badRequestsCounter, never()).increment();
        verify(requestsTooLargeCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Supplier<HttpResponse>>any());
    }

    @Test
    public void testHTTPRequestBadRequest() throws ExecutionException, InterruptedException {
        // Prepare
        AggregatedHttpRequest testBadRequest = generateBadHTTPRequest();

        // When
        AggregatedHttpResponse postResponse = logHTTPService.doPost(testBadRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, postResponse.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(requestTimeoutsCounter, never()).increment();
        verify(successRequestsCounter, never()).increment();
        verify(badRequestsCounter, times(1)).increment();
        verify(requestsTooLargeCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testBadRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Supplier<HttpResponse>>any());
    }

    @Test
    public void testHTTPRequestEntityTooLarge() throws ExecutionException, InterruptedException, JsonProcessingException {
        // Prepare
        AggregatedHttpRequest testTooLargeRequest = generateRandomValidHTTPRequest(TEST_BUFFER_CAPACITY + 1);

        // When
        AggregatedHttpResponse postResponse = logHTTPService.doPost(testTooLargeRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, postResponse.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(requestTimeoutsCounter, never()).increment();
        verify(successRequestsCounter, never()).increment();
        verify(badRequestsCounter, never()).increment();
        verify(requestsTooLargeCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testTooLargeRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Supplier<HttpResponse>>any());
    }

    @Test
    public void testHTTPRequestTimeout() throws InterruptedException, ExecutionException, JsonProcessingException {
        // Prepare
        AggregatedHttpRequest populateDataRequest = generateRandomValidHTTPRequest(3);
        AggregatedHttpResponse goodResponse = logHTTPService.doPost(populateDataRequest).aggregate().get();
        assertEquals(HttpStatus.OK, goodResponse.status());
        AggregatedHttpRequest timeoutRequest = generateRandomValidHTTPRequest(2);

        // When
        AggregatedHttpResponse timeoutPostResponse = logHTTPService.doPost(timeoutRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.REQUEST_TIMEOUT, timeoutPostResponse.status());
        verify(requestsReceivedCounter, times(2)).increment();
        verify(requestTimeoutsCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        verify(badRequestsCounter, never()).increment();
        verify(requestsTooLargeCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(2)).record(payloadLengthCaptor.capture());
        assertEquals(timeoutRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(2)).record(ArgumentMatchers.<Supplier<HttpResponse>>any());
    }

    private AggregatedHttpRequest generateRandomValidHTTPRequest(int numJson) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .build();
        List<Map<String, Object>> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(Collections.singletonMap("log", UUID.randomUUID().toString()));
        }
        String content = mapper.writeValueAsString(jsonList);
        HttpData httpData = HttpData.ofUtf8(content);
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private AggregatedHttpRequest generateBadHTTPRequest() throws ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .build();
        HttpData httpData = HttpData.ofUtf8("{");
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }
}