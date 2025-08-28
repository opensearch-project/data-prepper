/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LiveCaptureHandlerTest {

    @Mock
    private HttpExchange httpExchange;
    
    @Mock
    private EventFactory mockEventFactory;
    
    @Mock
    private EventBuilder mockEventBuilder;
    
    @Mock
    private Event mockEvent;
    
    @Mock
    private EventMetadata mockEventMetadata;

    private LiveCaptureHandler liveCaptureHandler;
    private LiveCaptureManager liveCaptureManager;
    private ByteArrayOutputStream responseStream;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        liveCaptureManager = new LiveCaptureManager();
        liveCaptureManager.initialize(false, 1.0);
        
        // Setup mock EventFactory chain with lenient stubbing
        lenient().when(mockEventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockEventBuilder);
        lenient().when(mockEventBuilder.withEventType(any())).thenReturn(mockEventBuilder);
        lenient().when(mockEventBuilder.withEventMetadataAttributes(any())).thenReturn(mockEventBuilder);
        lenient().when(mockEventBuilder.withData(any())).thenReturn(mockEventBuilder);
        lenient().when(mockEventBuilder.build()).thenReturn(mockEvent);
        
        // Setup mock Event for live capture operations
        lenient().when(mockEvent.getMetadata()).thenReturn(mockEventMetadata);
        lenient().when(mockEvent.toMap()).thenReturn(Map.of("testKey", "testValue"));
        lenient().when(mockEventMetadata.getAttribute("ingestionMethod")).thenReturn("injected");
        
        liveCaptureHandler = new LiveCaptureHandler(mockEventFactory, liveCaptureManager);
        responseStream = new ByteArrayOutputStream();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testHandleDisableLiveCapture() throws IOException {
        String requestBody = "{\"enabled\": false}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Live capture disabled"));
        assertThat(response, containsString("\"status\":\"success\""));
    }

    @Test
    void testHandleEnableLiveCaptureWithSampling() throws IOException {
        String requestBody = "{\"enabled\": true, \"mode\": \"sampling\", \"rate\": 2.5}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Live capture enabled"));
        assertThat(response, containsString("mode=sampling"));
        assertThat(response, containsString("rate=2.5"));
    }

    @Test
    void testHandleEnableLiveCaptureWithFiltering() throws IOException {
        String requestBody = "{\"enabled\": true, \"mode\": \"filter\", \"rate\": 1.0, \"filters\": {\"key1\": \"value1\", \"key2\": \"value2\"}}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Live capture enabled"));
        assertThat(response, containsString("mode=filter"));
    }

    @Test
    void testHandleEnableLiveCaptureWithDefaultValues() throws IOException {
        String requestBody = "{\"enabled\": true}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Live capture enabled"));
        assertThat(response, containsString("mode=sampling"));
        assertThat(response, containsString("rate=1.0"));
    }

    @Test
    void testHandleGetRequest() throws IOException {
        setupHttpExchange("GET", null);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("\"status\":\"success\""));
        assertThat(response, containsString("\"liveCaptureEnabled\":"));
        assertThat(response, containsString("\"currentRateLimit\":"));
    }
    
    @Test
    void testHandleInvalidMethod() throws IOException {
        setupHttpExchange("DELETE", null);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Only POST and GET methods are allowed"));
        assertThat(response, containsString("\"status\":\"error\""));
    }

    @Test
    void testHandleMissingEnabledField() throws IOException {
        String requestBody = "{\"mode\": \"sampling\"}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Missing required field: enabled"));
        assertThat(response, containsString("\"status\":\"error\""));
    }

    @Test
    void testHandleNegativeRate() throws IOException {
        String requestBody = "{\"enabled\": true, \"rate\": -1.0}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Rate must be positive"));
        assertThat(response, containsString("\"status\":\"error\""));
    }

    @Test
    void testHandleZeroRate() throws IOException {
        String requestBody = "{\"enabled\": true, \"rate\": 0.0}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Rate must be positive"));
    }

    @Test
    void testHandleInvalidJson() throws IOException {
        String requestBody = "{invalid json}";
        setupHttpExchange("POST", requestBody);
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Internal server error"));
        assertThat(response, containsString("\"status\":\"error\""));
    }

    @Test
    void testHandleInjectRequest() throws Exception {
        String requestBody = "{\"data\": {\"key1\": \"value1\", \"key2\": \"value2\"}, \"pipeline\": \"test-pipeline\"}";
        setupHttpExchange("POST", requestBody, "/livecapture/inject");
        
        Buffer<Record<Event>> mockBuffer = mock(Buffer.class);
        
        try (MockedStatic<LiveCaptureManager> mockedStatic = mockStatic(LiveCaptureManager.class)) {
            mockedStatic.when(() -> LiveCaptureManager.getPipelineBuffer("test-pipeline"))
                    .thenReturn(mockBuffer);
            
            liveCaptureHandler.handle(httpExchange);
            
            verify(mockBuffer).write(any(Record.class), anyInt());
            verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
            
            String response = responseStream.toString();
            assertThat(response, containsString("Event successfully injected into pipeline"));
            assertThat(response, containsString("\"status\":\"success\""));
            assertThat(response, containsString("test-pipeline"));
        }
    }

    @Test
    void testHandleInjectRequestMissingData() throws IOException {
        String requestBody = "{\"pipeline\": \"test-pipeline\"}";
        setupHttpExchange("POST", requestBody, "/livecapture/inject");
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Missing required field: data"));
        assertThat(response, containsString("\"status\":\"error\""));
    }

    @Test
    void testHandleInjectRequestMissingPipeline() throws IOException {
        String requestBody = "{\"data\": {\"key1\": \"value1\"}}";
        setupHttpExchange("POST", requestBody, "/livecapture/inject");
        
        liveCaptureHandler.handle(httpExchange);
        
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        
        String response = responseStream.toString();
        assertThat(response, containsString("Missing required field: pipeline"));
        assertThat(response, containsString("\"status\":\"error\""));
    }

    @Test
    void testHandleInjectRequestPipelineNotFound() throws IOException {
        String requestBody = "{\"data\": {\"key1\": \"value1\"}, \"pipeline\": \"nonexistent-pipeline\"}";
        setupHttpExchange("POST", requestBody, "/livecapture/inject");
        
        try (MockedStatic<LiveCaptureManager> mockedStatic = mockStatic(LiveCaptureManager.class)) {
            mockedStatic.when(() -> LiveCaptureManager.getPipelineBuffer("nonexistent-pipeline"))
                    .thenReturn(null);
            
            liveCaptureHandler.handle(httpExchange);
            
            verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_NOT_FOUND), anyLong());
            
            String response = responseStream.toString();
            assertThat(response, containsString("Pipeline 'nonexistent-pipeline' not found"));
            assertThat(response, containsString("\"status\":\"error\""));
        }
    }

    @Test
    void testHandleInjectRequestTimeout() throws Exception {
        String requestBody = "{\"data\": {\"key1\": \"value1\"}, \"pipeline\": \"test-pipeline\"}";
        setupHttpExchange("POST", requestBody, "/livecapture/inject");
        
        Buffer<Record<Event>> mockBuffer = mock(Buffer.class);
        
        // Configure mock to throw TimeoutException
        doThrow(new TimeoutException("Buffer write timeout")).when(mockBuffer).write(any(Record.class), anyInt());
        
        try (MockedStatic<LiveCaptureManager> mockedStatic = mockStatic(LiveCaptureManager.class)) {
            mockedStatic.when(() -> LiveCaptureManager.getPipelineBuffer("test-pipeline"))
                    .thenReturn(mockBuffer);
            
            liveCaptureHandler.handle(httpExchange);
            
            verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_CLIENT_TIMEOUT), anyLong());
            
            String response = responseStream.toString();
            assertThat(response, containsString("Timeout writing to pipeline buffer"));
            assertThat(response, containsString("\"status\":\"error\""));
        }
    }

    @Test
    void testHandleInjectRequestInvalidJson() throws IOException {
        String requestBody = "{\"data\": {\"key1\": \"value1\"}, \"pipeline\": \"test-pipeline\"}";
        setupHttpExchange("POST", requestBody, "/livecapture/inject");
        
        Buffer<Record<Event>> mockBuffer = mock(Buffer.class);
        
        try (MockedStatic<LiveCaptureManager> mockedStatic = mockStatic(LiveCaptureManager.class)) {
            mockedStatic.when(() -> LiveCaptureManager.getPipelineBuffer("test-pipeline"))
                    .thenReturn(mockBuffer);
            
            // Test with malformed JSON that will cause JSON parsing error
            responseStream.reset();
            String badRequestBody = "{\"data\": {key1: value1}, \"pipeline\": \"test-pipeline\"}"; // Invalid JSON
            setupHttpExchange("POST", badRequestBody, "/livecapture/inject");
            
            liveCaptureHandler.handle(httpExchange);
            
            verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), anyLong());
            
            String response = responseStream.toString();
            assertThat(response, containsString("Internal server error"));
            assertThat(response, containsString("\"status\":\"error\""));
        }
    }

    private void setupHttpExchange(String method, String requestBody) throws IOException {
        setupHttpExchange(method, requestBody, "/livecapture");
    }

    private void setupHttpExchange(String method, String requestBody, String path) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(method);
        if (requestBody != null) {
            when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(requestBody.getBytes()));
        }
        when(httpExchange.getResponseBody()).thenReturn(responseStream);
        when(httpExchange.getResponseHeaders()).thenReturn(new com.sun.net.httpserver.Headers());
        
        // Mock URI with the specified path
        try {
            when(httpExchange.getRequestURI()).thenReturn(new URI(path));
        } catch (Exception e) {
            // Ignore URI creation errors in tests
        }
    }
}