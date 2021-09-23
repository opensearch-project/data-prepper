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

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogHTTPServiceTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int TEST_BUFFER_CAPACITY = 3;
    private static final int TEST_TIMEOUT_IN_MILLIS = 500;

    private LogHTTPService logHTTPService;

    @BeforeEach
    public void setUp() {
        Buffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_CAPACITY, 8, "test-pipeline");
        logHTTPService = new LogHTTPService(TEST_TIMEOUT_IN_MILLIS, blockingBuffer);
    }

    @Test
    public void testHTTPRequestSuccess() throws InterruptedException, ExecutionException, JsonProcessingException {
        // Prepare
        AggregatedHttpRequest testGetRequest = generateRandomValidHTTPRequest(HttpMethod.GET, 1);
        AggregatedHttpRequest testPostRequest = generateRandomValidHTTPRequest(HttpMethod.POST, 2);

        // When
        AggregatedHttpResponse getResponse = logHTTPService.doGet(testGetRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.OK, getResponse.status());

        // When
        AggregatedHttpResponse postResponse = logHTTPService.doPost(testPostRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.OK, postResponse.status());
    }

    @Test
    public void testHTTPRequestBadRequest() throws ExecutionException, InterruptedException {
        // Prepare
        AggregatedHttpRequest testBadGetRequest = generateBadHTTPRequest(HttpMethod.GET);
        AggregatedHttpRequest testBadPostRequest = generateBadHTTPRequest(HttpMethod.POST);

        // When
        AggregatedHttpResponse getResponse = logHTTPService.doGet(testBadGetRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, getResponse.status());

        // When
        AggregatedHttpResponse postResponse = logHTTPService.doPost(testBadPostRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, postResponse.status());
    }

    @Test
    public void testHTTPRequestTimeout() throws InterruptedException, ExecutionException, JsonProcessingException {
        // Prepare
        AggregatedHttpRequest populateDataRequest = generateRandomValidHTTPRequest(HttpMethod.GET, 2);
        AggregatedHttpResponse getResponse = logHTTPService.doGet(populateDataRequest).aggregate().get();
        assertEquals(HttpStatus.OK, getResponse.status());
        AggregatedHttpRequest timeoutRequest = generateRandomValidHTTPRequest(HttpMethod.GET, 2);

        // When
        AggregatedHttpResponse timeoutResponse = logHTTPService.doGet(timeoutRequest).aggregate().get();

        // Then
        assertEquals(HttpStatus.REQUEST_TIMEOUT, timeoutResponse.status());
    }

    private AggregatedHttpRequest generateRandomValidHTTPRequest(HttpMethod httpMethod, int numJson) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(httpMethod)
                .path("/log/ingest")
                .build();
        List<Map<String, Object>> jsonList = new ArrayList<>();
        for (int i = 0; i < numJson; i++) {
            jsonList.add(new HashMap<String, Object>() {{ put("log", UUID.randomUUID().toString()); }});
        }
        String content = mapper.writeValueAsString(jsonList);
        HttpData httpData = HttpData.ofUtf8(content);
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private AggregatedHttpRequest generateBadHTTPRequest(HttpMethod httpMethod) throws ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(httpMethod)
                .path("/log/ingest")
                .build();
        HttpData httpData = HttpData.ofUtf8("{");
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }
}