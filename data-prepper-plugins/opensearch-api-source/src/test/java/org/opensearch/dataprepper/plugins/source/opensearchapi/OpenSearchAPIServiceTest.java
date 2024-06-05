/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.event.Event;
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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class OpenSearchAPIServiceTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int TEST_BUFFER_CAPACITY = 15;
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

    private OpenSearchAPIService openSearchAPIService;

    @BeforeEach
    public void setUp() throws Exception {
        lenient().when(pluginMetrics.counter(openSearchAPIService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        lenient().when(pluginMetrics.counter(openSearchAPIService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        lenient().when(pluginMetrics.summary(openSearchAPIService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        lenient().when(pluginMetrics.timer(openSearchAPIService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        lenient().when(serviceRequestContext.isTimedOut()).thenReturn(false);
        lenient().when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(
                (Answer<HttpResponse>) invocation -> {
                    final Object[] args = invocation.getArguments();
                    @SuppressWarnings("unchecked")
                    final Callable<HttpResponse> callable = (Callable<HttpResponse>) args[0];
                    return callable.call();
                }
        );

        Buffer<Record<Event>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_CAPACITY, 8, "test-pipeline");
        openSearchAPIService = new OpenSearchAPIService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPISuccess(boolean testBulkRequestAPIWithIndexInPath) throws Exception {

        AggregatedHttpRequest testRequest;
        AggregatedHttpResponse postResponse;

        if (testBulkRequestAPIWithIndexInPath) {
            // Prepare
            testRequest = generateRandomValidBulkRequestWithNoIndexInBody(2);
            // When
            postResponse = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, Optional.empty(),
                    Optional.ofNullable("pipeline-1"), Optional.ofNullable("routing-1")).aggregate().get();
        } else {
            // Prepare
            testRequest = generateRandomValidBulkRequest(2);
            // When
            postResponse = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    Optional.empty(), Optional.empty()).aggregate().get();
        }

        // Then
        assertEquals(HttpStatus.OK, postResponse.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPISuccessWithMultipleBulkActions(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        // Prepare
        AggregatedHttpRequest testRequest = generateGoodBulkRequestWithMultipleActions(2);

        // When
        AggregatedHttpResponse postResponse;
        if (testBulkRequestAPIWithIndexInPath) {
            postResponse = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, Optional.empty(),
                    Optional.ofNullable("pipeline-1"), Optional.ofNullable("routing-1")).aggregate().get();
        } else {
            postResponse = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    Optional.empty(), Optional.empty()).aggregate().get();
        }

        // Then
        assertEquals(HttpStatus.OK, postResponse.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIBadRequestWithEmpty(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        testBadRequestWithPayload(testBulkRequestAPIWithIndexInPath, "");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIBadRequestWithInvalidPayload(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Collections.singletonMap("_index", "test-index"))));
        }
        testBadRequestWithPayload(testBulkRequestAPIWithIndexInPath, String.join("\n", jsonList));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIBadRequestWithInvalidPayload2(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("text", Collections.singletonMap("x", "test"))));
        }
        testBadRequestWithPayload(testBulkRequestAPIWithIndexInPath, String.join("\n", jsonList));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIBadRequestWithInvalidPayload3(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        List<String> jsonList = new ArrayList<>();
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Collections.singletonMap("_index", "test-index"))));
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Collections.singletonMap("_index", "test-index"))));

        testBadRequestWithPayload(testBulkRequestAPIWithIndexInPath, String.join("\n", jsonList));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIEntityTooLarge(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        // Prepare
        AggregatedHttpRequest testTooLargeRequest = generateRandomValidBulkRequest(TEST_BUFFER_CAPACITY + 1);

        // When
        if (testBulkRequestAPIWithIndexInPath) {
            assertThrows(SizeOverflowException.class, () -> openSearchAPIService.doPostBulkIndex(serviceRequestContext, testTooLargeRequest, Optional.empty(),
                    Optional.empty(), Optional.empty()).aggregate().get());
        } else {
            assertThrows(SizeOverflowException.class, () -> openSearchAPIService.doPostBulk(serviceRequestContext, testTooLargeRequest, Optional.empty(),
                    Optional.empty()).aggregate().get());
        }

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testTooLargeRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    @Test
    public void testBulkRequestWithIndexAPIRequestTimeout() throws Exception {
        // Prepare
        AggregatedHttpRequest populateDataRequest = generateRandomValidBulkRequest(3);

        lenient().when(serviceRequestContext.isTimedOut()).thenReturn(true);

        AggregatedHttpResponse response = openSearchAPIService.doPostBulkIndex(serviceRequestContext, populateDataRequest, Optional.empty(),
                Optional.empty(), Optional.empty()).aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, response.status());

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
    }

    @Test
    public void testBulkRequestAPIRequestTimeout() throws Exception {
        // Prepare
        AggregatedHttpRequest populateDataRequest = generateRandomValidBulkRequest(3);

        lenient().when(serviceRequestContext.isTimedOut()).thenReturn(true);
        AggregatedHttpResponse response = openSearchAPIService.doPostBulk(serviceRequestContext, populateDataRequest, Optional.empty(),
                Optional.empty()).aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, response.status());

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
    }

    private void testBadRequestWithPayload(boolean testBulkRequestAPIWithIndexInPath, String requestBody) throws Exception {
        // Prepare
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/opensearch")
                .build();

        HttpData httpData = HttpData.ofUtf8(requestBody);
        AggregatedHttpRequest testBadRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();
        // When
        if (testBulkRequestAPIWithIndexInPath) {
            assertThrows(IOException.class, () -> openSearchAPIService.doPostBulkIndex(serviceRequestContext, testBadRequest, Optional.empty(),
                    Optional.empty(), Optional.empty()).aggregate().get());
        } else {
            assertThrows(IOException.class, () -> openSearchAPIService.doPostBulk(serviceRequestContext, testBadRequest, Optional.empty(),
                    Optional.empty()).aggregate().get());
        }

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, never()).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertEquals(testBadRequest.content().length(), Math.round(payloadLengthCaptor.getValue()));
        verify(requestProcessDuration, times(1)).recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any());
    }

    private AggregatedHttpRequest generateRandomValidBulkRequest(int numJson) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/opensearch")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "test-index", "_id", "123"))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private AggregatedHttpRequest generateRandomValidBulkRequestWithNoIndexInBody(int numJson) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/opensearch")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_id", "123"))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private AggregatedHttpRequest generateGoodBulkRequestWithMultipleActions(int numJson) throws JsonProcessingException, ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/opensearch")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "test-index", "_id", "123"))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("delete", Map.of("_index", "test-index", "_id", "124"))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("create", Map.of("_index", "test-index", "_id", "125"))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("update", Map.of("_index", "test-index", "_id", "126"))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }
}
