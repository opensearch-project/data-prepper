/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * HttpHandler for the /livecapture REST endpoint.
 * Handles enabling/disabling live capture and configuring capture parameters.
 * Also provides GET endpoint to retrieve captured live entries.
 */
public class LiveCaptureHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LiveCaptureHandler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final LiveCaptureManager liveCaptureManager;
    private final EventFactory eventFactory;

    public LiveCaptureHandler(final EventFactory eventFactory, final LiveCaptureManager liveCaptureManager) {
        this.liveCaptureManager = liveCaptureManager;
        this.eventFactory = eventFactory;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        if (HttpMethod.POST.equals(requestMethod)) {
            if (path.endsWith("/inject")) {
                handleInjectRequest(exchange);
            } else {
                handlePostRequest(exchange);
            }
        } else if (HttpMethod.GET.equals(requestMethod)) {
            handleGetRequest(exchange);
        } else {
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_METHOD, "Only POST and GET methods are allowed");
        }
    }

    private void handlePostRequest(final HttpExchange exchange) throws IOException {
        try {
            JsonNode requestBody = parseRequestBody(exchange);

            if (!requestBody.has("enabled")) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Missing required field: enabled");
                return;
            }
            
            boolean enabled = requestBody.get("enabled").asBoolean();
            if (!enabled) {
                boolean preserveFilters = requestBody.has("preserve_filters") && requestBody.get("preserve_filters").asBoolean();
                liveCaptureManager.setEnabled(false, !preserveFilters); // clearFilters = !preserveFilters
                String message = preserveFilters ? "Live capture disabled (filters preserved)" : "Live capture disabled (filters cleared)";
                sendSuccessResponse(exchange, message);
                return;
            }

            String mode = requestBody.has("mode") ? requestBody.get("mode").asText() : "sampling";
            double rate = requestBody.has("rate") ? requestBody.get("rate").asDouble() : 1.0;

            if (rate <= 0) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Rate must be positive");
                return;
            }

            liveCaptureManager.setRateLimit(rate);
            configureFilters(mode, requestBody);
            liveCaptureManager.setEnabled(true);

            String message = String.format("Live capture enabled with mode=%s, rate=%.1f", 
                mode, rate);
            sendSuccessResponse(exchange, message);
        } catch (Exception e) {
            LOG.error("Error handling live capture request", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal server error");
        }
    }

    private void handleGetRequest(final HttpExchange exchange) throws IOException {
        try {
            Map<String, Object> response = Map.of(
                "status", "success",
                "liveCaptureEnabled", liveCaptureManager.isEnabled(),
                "currentRateLimit", liveCaptureManager.getRateLimit()
            );

            sendJsonResponse(exchange, response);
        } catch (Exception e) {
            LOG.error("Error handling live capture GET request", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal server error");
        }
    }

    /**
     * Parses the JSON request body from the HTTP exchange.
     *
     * @param exchange the HTTP exchange
     * @return the parsed JSON request body
     * @throws IOException if parsing fails
     */
    private JsonNode parseRequestBody(final HttpExchange exchange) throws IOException {
        try (InputStream inputStream = exchange.getRequestBody()) {
            return OBJECT_MAPPER.readTree(inputStream);
        }
    }

    private void sendSuccessResponse(final HttpExchange exchange, final String message) throws IOException {
        Map<String, Object> response = Map.of("status", "success", "message", message);
        sendJsonResponse(exchange, response);
        LOG.info("Live capture API: {}", message);
    }

    private void sendErrorResponse(final HttpExchange exchange, final int statusCode, final String message) throws IOException {
        Map<String, Object> response = Map.of("status", "error", "message", message);
        byte[] responseBytes = OBJECT_MAPPER.writeValueAsString(response).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.getResponseBody().close();
        LOG.warn("Live capture API error: {} - {}", statusCode, message);
    }

    
    private void configureFilters(String mode, JsonNode requestBody) {
        if ("filter".equals(mode) && requestBody.has("filters")) {
            liveCaptureManager.clearFilters();
            JsonNode filters = requestBody.get("filters");
            if (filters.isObject()) {
                filters.fields().forEachRemaining(entry -> 
                    liveCaptureManager.addFilter(entry.getKey(), entry.getValue().asText()));
            }
        } else if ("sampling".equals(mode)) {
            liveCaptureManager.clearFilters();
        }
    }
    
    private Map<String, String> parseQueryParameters(final HttpExchange exchange) {
        Map<String, String> params = new HashMap<>();
        try {
            List<NameValuePair> queryParams = URLEncodedUtils.parse(exchange.getRequestURI(), Charset.defaultCharset());
            for (NameValuePair param : queryParams) {
                params.put(param.getName(), param.getValue() != null ? param.getValue() : "");
            }
        } catch (Exception e) {
            LOG.warn("Error parsing query parameters: {}", e.getMessage());
        }
        return params;
    }

    
    private boolean validateInjectRequest(final JsonNode requestBody, final HttpExchange exchange) throws IOException {
        if (!requestBody.has("data")) {
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Missing required field: data");
            return false;
        }
        if (!requestBody.has("pipeline")) {
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Missing required field: pipeline");
            return false;
        }
        return true;
    }

    /**
     * Sends a JSON response with the given data.
     *
     * @param exchange the HTTP exchange
     * @param data the data to serialize as JSON
     * @throws IOException if sending the response fails
     */
    private void sendJsonResponse(final HttpExchange exchange, final Object data) throws IOException {
        byte[] responseBytes = OBJECT_MAPPER.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.getResponseBody().close();

        LOG.info("Live capture API response: status={}, size={} bytes", HttpURLConnection.HTTP_OK, responseBytes.length);
    }



    /**
     * Handles POST requests for injecting events into pipelines.
     * Expected JSON format: {"data": {...}, "pipeline": "pipeline_name"}
     */
    private void handleInjectRequest(final HttpExchange exchange) throws IOException {
        try {
            JsonNode requestBody = parseRequestBody(exchange);
            if (!validateInjectRequest(requestBody, exchange)) return;

            String pipelineName = requestBody.get("pipeline").asText();
            JsonNode data = requestBody.get("data");

            Buffer<Record<Event>> pipelineBuffer = liveCaptureManager.getPipelineBuffer(pipelineName);
            if (pipelineBuffer == null) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, 
                    String.format("Pipeline '%s' not found. Available pipelines: %s", 
                        pipelineName, liveCaptureManager.getAvailablePipelines()));
                return;
            }

            Event event = eventFactory.eventBuilder(EventBuilder.class)
                    .withEventType("injected")
                    .withEventMetadataAttributes(Map.of("ingestionMethod", "injected"))
                    .withData(OBJECT_MAPPER.convertValue(data, Map.class))
                    .build();
            
            LiveCaptureManager.setLiveCapture(event, true);
            LiveCaptureManager.addLiveCaptureEntry(event, "Source",
                "Event injected into pipeline via /livecapture/inject API", null, pipelineName);

            try {
                pipelineBuffer.write(new Record<>(event), 5000);
            } catch (TimeoutException e) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_CLIENT_TIMEOUT, 
                    "Timeout writing to pipeline buffer: " + e.getMessage());
                return;
            }

            sendSuccessResponse(exchange, String.format("Event successfully injected into pipeline '%s'", pipelineName));
            LOG.info("Event injected into pipeline '{}' via live capture API", pipelineName);
        } catch (Exception e) {
            LOG.error("Error handling live capture inject request", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal server error: " + e.getMessage());
        }
    }

}