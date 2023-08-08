/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.server.ServiceRequestContext;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    private Counter successRequestsCounter;

    @Mock
    private DistributionSummary payloadSizeSummary;

    @Mock
    private Timer requestProcessDuration;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    private LogHTTPService logHTTPService;

    @BeforeEach
    public void setUp() throws Exception {
        when(pluginMetrics.counter(LogHTTPService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(pluginMetrics.counter(LogHTTPService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(pluginMetrics.summary(LogHTTPService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(pluginMetrics.timer(LogHTTPService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(
                (Answer<HttpResponse>) invocation -> {
                    final Object[] args = invocation.getArguments();
                    @SuppressWarnings("unchecked")
                    final Callable<HttpResponse> callable = (Callable<HttpResponse>) args[0];
                    return callable.call();
                }
        );

        Buffer<Record<Log>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_CAPACITY, 8, "test-pipeline");
        logHTTPService = new LogHTTPService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
    }

    @Test
    public void testHTTPRequestSuccess() throws Exception {
        // Prepare
        AggregatedHttpRequest testRequest = generateRandomValidHTTPRequest(2);

        // When
        AggregatedHttpResponse postResponse = logHTTPService.doPost(serviceRequestContext, testRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.OK, postResponse.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    @Test
    public void testHTTPRequestBadRequest() throws Exception {
        // Prepare
        AggregatedHttpRequest testBadRequest = generateBadHTTPRequest();

        // When
        assertThrows(IOException.class, () -> logHTTPService.doPost(serviceRequestContext, testBadRequest).aggregate().get());

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testBadRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    @Test
    public void testHTTPRequestEntityTooLarge() throws Exception {
        // Prepare
        AggregatedHttpRequest testTooLargeRequest = generateRandomValidHTTPRequest(TEST_BUFFER_CAPACITY + 1);

        // When
        assertThrows(SizeOverflowException.class, () -> logHTTPService.doPost(serviceRequestContext, testTooLargeRequest).aggregate().get());

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testTooLargeRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    @Test
    public void testHTTPRequestTimeout() throws Exception {
        // Prepare
        AggregatedHttpRequest populateDataRequest = generateRandomValidHTTPRequest(3);
        AggregatedHttpResponse goodResponse = logHTTPService.doPost(serviceRequestContext, populateDataRequest).aggregate().get();
        assertEquals(HttpStatus.OK, goodResponse.status());
        AggregatedHttpRequest timeoutRequest = generateRandomValidHTTPRequest(2);

        // When
        assertThrows(TimeoutException.class, () -> logHTTPService.doPost(serviceRequestContext, timeoutRequest).aggregate().get());

        // Then
        verify(requestsReceivedCounter, times(2)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(2)).record(payloadLengthCaptor.capture());
        assertEquals(timeoutRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(2)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
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