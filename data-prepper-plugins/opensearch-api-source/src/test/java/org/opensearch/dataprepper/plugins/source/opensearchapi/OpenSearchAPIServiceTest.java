/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

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
import com.linecorp.armeria.server.ServiceRequestContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        lenient().when(pluginMetrics.counter(OpenSearchAPIService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        lenient().when(pluginMetrics.counter(OpenSearchAPIService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        lenient().when(pluginMetrics.summary(OpenSearchAPIService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        lenient().when(pluginMetrics.timer(OpenSearchAPIService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        lenient().when(serviceRequestContext.isTimedOut()).thenReturn(false);
        lenient().when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(
                (Answer<HttpResponse>) invocation -> {
                    final Object[] args = invocation.getArguments();
                    @SuppressWarnings("unchecked") final Callable<HttpResponse> callable = (Callable<HttpResponse>) args[0];
                    return callable.call();
                }
        );

        Buffer<Record<Event>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_CAPACITY, 8, "test-pipeline");
        openSearchAPIService = new OpenSearchAPIService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
    }

    @ParameterizedTest
    @CsvSource(value = {
            "false, null, null, null",
            "false, null, \"\", \"\"",
            "false, null, \"     \", \"    \"",
            "false, null, \"pipeline-1\", \"routing-1\"",
            "false, \"index-1\", \"pipeline-1\", \"routing-1\"",
            "true, null, null, null",
            "true, null, \"\", \"\"",
            "true, null, \"     \", \"    \"",
            "true, null, \"pipeline-1\", \"routing-1\"",
            "true, \"index-1\", \"pipeline-1\", \"routing-1\""
    })
    public void testBulkRequestAPIWithQueryParams(boolean testBulkRequestAPIWithIndexInPath, final String index, final String pipeline, final String routing) throws Exception {

        AggregatedHttpRequest testRequest;
        AggregatedHttpResponse postResponse;

        if (testBulkRequestAPIWithIndexInPath) {
            // Prepare
            testRequest = generateRandomValidBulkRequestWithNoIndexInBody(2);
            // When
            postResponse = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, index,
                    pipeline, routing).aggregate().get();
        } else {
            // Prepare
            testRequest = generateRandomValidBulkRequest(2);
            // When
            postResponse = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    pipeline, routing).aggregate().get();
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
    @CsvSource(value = {
            "false, null, null, null",
            "false, null, \"\", \"\"",
            "false, null, \"     \", \"    \"",
            "false, null, \"pipeline-1\", \"routing-1\"",
            "false, \"index-1\", \"pipeline-1\", \"routing-1\"",
            "true, null, null, null",
            "true, null, \"\", \"\"",
            "true, null, \"     \", \"    \"",
            "true, null, \"pipeline-1\", \"routing-1\"",
            "true, \"index-1\", \"pipeline-1\", \"routing-1\""
    })
    public void testBulkRequestAPISuccessWithMultipleBulkActions(boolean testBulkRequestAPIWithIndexInPath, final String index, final String pipeline, final String routing) throws Exception {
        // Prepare
        AggregatedHttpRequest testRequest = generateGoodBulkRequestWithMultipleActions(2);

        // When

        AggregatedHttpResponse postResponse;
        if (testBulkRequestAPIWithIndexInPath) {
            postResponse = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, index,
                    pipeline, routing).aggregate().get();
        } else {
            postResponse = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    pipeline, routing).aggregate().get();
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
    public void testBulkRequestAPIWithByteBuffer(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        Buffer<Record<Event>> blockingBuffer = mock(BlockingBuffer.class);
        OpenSearchAPIService openSearchAPIService = new OpenSearchAPIService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
        when(blockingBuffer.isByteBuffer()).thenReturn(true);

        AggregatedHttpRequest testRequest = generateGoodBulkRequestWithMultipleActions(2);
        if (testBulkRequestAPIWithIndexInPath) {
            openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, null,
                    null, null).aggregate().get();
        } else {
            openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    null, null).aggregate().get();
        }
        verify(blockingBuffer, times(1)).writeBytes(eq(testRequest.content().array()), eq(null), eq(TEST_TIMEOUT_IN_MILLIS));
    }

    @Test
    public void testBulkRequestAPIEmptyIndex() throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "\t\r\n", "_id", UUID.randomUUID().toString()))));
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        AggregatedHttpRequest testRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        Buffer<Record<Event>> blockingBuffer = mock(BlockingBuffer.class);
        OpenSearchAPIService openSearchAPIService = new OpenSearchAPIService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
        openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                null, null).aggregate().get();
    }

    // Decoder is lenient: treats the second action line as document data for the first action,
    // matching OpenSearch's own tolerant bulk parsing behavior.
    @Test
    public void testBulkRequestAPIWithMissingDocRowSucceeds() throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "index", "_id", UUID.randomUUID().toString()))));
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "index", "_id", UUID.randomUUID().toString()))));
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        AggregatedHttpRequest testRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        Buffer<Record<Event>> blockingBuffer = mock(BlockingBuffer.class);
        OpenSearchAPIService openSearchAPIService = new OpenSearchAPIService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
        AggregatedHttpResponse response = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                null, null).aggregate().get();
        assertEquals(HttpStatus.OK, response.status());
    }

    // Decoder skips an action line that has no following document line (EOF reached), producing zero events.
    @Test
    public void testBulkRequestAPIWithSingleActionLineSucceeds() throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "index", "_id", UUID.randomUUID().toString()))));
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        AggregatedHttpRequest testRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        Buffer<Record<Event>> blockingBuffer = mock(BlockingBuffer.class);
        OpenSearchAPIService openSearchAPIService = new OpenSearchAPIService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer, pluginMetrics);
        AggregatedHttpResponse response = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                null, null).aggregate().get();
        assertEquals(HttpStatus.OK, response.status());
    }

    // Empty payload produces zero events and returns 200.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIWithEmptyPayloadSucceeds(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();

        HttpData httpData = HttpData.ofUtf8("");
        AggregatedHttpRequest testRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        AggregatedHttpResponse response;
        if (testBulkRequestAPIWithIndexInPath) {
            response = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, null,
                    null, null).aggregate().get();
        } else {
            response = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    null, null).aggregate().get();
        }
        assertEquals(HttpStatus.OK, response.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
    }

    // Decoder skips JSON lines with no valid bulk action key, producing zero events.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIWithEmptyMapPayloadSucceeds(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.emptyMap()));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        AggregatedHttpRequest testRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        AggregatedHttpResponse response;
        if (testBulkRequestAPIWithIndexInPath) {
            response = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, null,
                    null, null).aggregate().get();
        } else {
            response = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    null, null).aggregate().get();
        }
        assertEquals(HttpStatus.OK, response.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
    }

    // Decoder skips JSON lines with no valid bulk action key, producing zero events.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIWithNonActionPayloadSucceeds(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("text", Collections.singletonMap("x", "test"))));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        AggregatedHttpRequest testRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        AggregatedHttpResponse response;
        if (testBulkRequestAPIWithIndexInPath) {
            response = openSearchAPIService.doPostBulkIndex(serviceRequestContext, testRequest, null,
                    null, null).aggregate().get();
        } else {
            response = openSearchAPIService.doPostBulk(serviceRequestContext, testRequest,
                    null, null).aggregate().get();
        }
        // Decoder skips lines with no valid bulk action
        assertEquals(HttpStatus.OK, response.status());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIBadRequestWithInvalidJson(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();

        HttpData httpData = HttpData.ofUtf8("this is not valid json\n");
        AggregatedHttpRequest testBadRequest = HttpRequest.of(requestHeaders, httpData).aggregate().get();

        if (testBulkRequestAPIWithIndexInPath) {
            assertThrows(IOException.class, () -> openSearchAPIService.doPostBulkIndex(serviceRequestContext, testBadRequest, null,
                    null, null).aggregate().get());
        } else {
            assertThrows(IOException.class, () -> openSearchAPIService.doPostBulk(serviceRequestContext, testBadRequest, null,
                    null).aggregate().get());
        }

        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, never()).increment();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIEntityTooLarge(boolean testBulkRequestAPIWithIndexInPath) throws Exception {
        // Prepare
        AggregatedHttpRequest testTooLargeRequest = generateRandomValidBulkRequest(TEST_BUFFER_CAPACITY + 1);

        // When
        if (testBulkRequestAPIWithIndexInPath) {
            assertThrows(SizeOverflowException.class, () -> openSearchAPIService.doPostBulkIndex(serviceRequestContext, testTooLargeRequest, null,
                    null, null).aggregate().get());
        } else {
            assertThrows(SizeOverflowException.class, () -> openSearchAPIService.doPostBulk(serviceRequestContext, testTooLargeRequest, null,
                    null).aggregate().get());
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

        AggregatedHttpResponse response = openSearchAPIService.doPostBulkIndex(serviceRequestContext, populateDataRequest, null,
                null, null).aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, response.status());

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
    }

    @Test
    public void testBulkRequestAPIRequestTimeout() throws Exception {
        // Prepare
        AggregatedHttpRequest populateDataRequest = generateRandomValidBulkRequest(3);

        lenient().when(serviceRequestContext.isTimedOut()).thenReturn(true);
        AggregatedHttpResponse response = openSearchAPIService.doPostBulk(serviceRequestContext, populateDataRequest, null,
                null).aggregate().get();
        assertEquals(HttpStatus.REQUEST_TIMEOUT, response.status());

        // Then
        verify(requestsReceivedCounter, times(1)).increment();
    }

    private AggregatedHttpRequest generateRandomValidBulkRequest(int numJson) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "test-index", "_id", UUID.randomUUID().toString()))));
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
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private AggregatedHttpRequest generateGoodBulkRequestWithMultipleActions(int numJson) throws JsonProcessingException, ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/")
                .build();
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "test-index", "_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "", "_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", "           ", "_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("delete", Map.of("_index", "test-index", "_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("create", Map.of("_index", "test-index", "_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("update", Map.of("_index", "test-index", "_id", UUID.randomUUID().toString()))));
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("log", UUID.randomUUID().toString())));
        }
        HttpData httpData = HttpData.ofUtf8(String.join("\n", jsonList));
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }
}
