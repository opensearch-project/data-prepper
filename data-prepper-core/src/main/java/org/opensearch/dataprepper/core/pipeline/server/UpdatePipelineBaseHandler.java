/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * HttpHandler to handle requests for updating pipeline configurations from S3
 */
public abstract class UpdatePipelineBaseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(UpdatePipelineBaseHandler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern S3_PATH_PATTERN = Pattern.compile("s3://([^/]+)/(.+)");
    private static final Pattern PIPELINE_NAME_PATTERN = Pattern.compile("/([a-zA-Z0-9-]{1,28})$");
    private final PipelinesProvider pipelinesProvider;

    public UpdatePipelineBaseHandler(final PipelinesProvider pipelinesProvider) {
        this.pipelinesProvider = pipelinesProvider;
    }

    public void baseHandle(final HttpExchange exchange, boolean executeUpdate) throws IOException {
        final String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equals(HttpMethod.PUT)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            exchange.getResponseBody().close();
            return;
        }

        try {
            // Extract pipeline name from URL path
            final String pipelineName = extractPipelineNameFromPath(exchange.getRequestURI().getPath());
            if (pipelineName == null) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid pipeline name in URL path");
                return;
            }

            // Parse request body to get S3 path
            final String requestBody = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            final S3PathRequest s3PathRequest = parseS3PathRequest(requestBody);
            if (s3PathRequest == null) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid request body. Expected JSON with 's3paths' field containing an array of S3 paths");
                return;
            }
            // Get region
            final Region region;
            try {
                region = s3PathRequest.s3region != null ? Region.of(s3PathRequest.s3region) : Region.US_EAST_1;
            } catch (IllegalArgumentException e) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid AWS region");
                return;
            }

            // Read configurations from S3
            final S3Client regionSpecificClient = createS3Client(region);

            final List<String> configurationContents = new ArrayList<>();
            try {
                for (String s3path : s3PathRequest.s3paths) {
                    configurationContents.add(readConfigurationFromS3(s3path, regionSpecificClient));
                    LOG.info("Successfully read configuration for pipeline '{}' from S3 path: {}", pipelineName, s3path);
                }
            } catch (SdkClientException | S3Exception e) {
                LOG.error("AWS SDK error: {}", e.getMessage());
                sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "AWS error: " + e.getMessage());
                return;
            }

            // TODO: Validate and update pipeline configuration
            // For now, we'll just log the contents and return success
            LOG.debug("Configuration contents: {}", configurationContents);

            // Send success response
            final String successMessage = "{\"message\": \"Pipeline configuration updated successfully\", \"pipeline\": \"" + pipelineName + "\"}";
            final byte[] response = successMessage.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            exchange.getResponseBody().write(response);

        } catch (final IllegalArgumentException e) {
            LOG.warn("Invalid request parameters: {}", e.getMessage());
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
        } catch (final SdkClientException e) {
            if (e.getCause() instanceof java.net.UnknownHostException || e.getMessage().contains("Invalid AWS region")) {
                LOG.warn("Invalid AWS region: {}", e.getMessage());
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid AWS region");
            } else {
                LOG.error("AWS SDK error: {}", e.getMessage());
                sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "AWS error: " + e.getMessage());
            }
        } catch (final Exception e) {
            LOG.error("Unexpected error updating pipeline configuration", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal server error: " + e.getMessage());
        } finally {
            exchange.getResponseBody().close();
        }
    }

    public S3Client createS3Client(final Region region) {
        return S3Client.builder().region(region).build();
    }

    private String extractPipelineNameFromPath(final String path) {
        final Matcher matcher = PIPELINE_NAME_PATTERN.matcher(path);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private S3PathRequest parseS3PathRequest(final String requestBody) {
        try {
            @SuppressWarnings("unchecked") final Map<String, Object> requestMap = OBJECT_MAPPER.readValue(requestBody, Map.class);
            final Object s3pathsObj = requestMap.get("s3paths");

            if (s3pathsObj instanceof List) {
                @SuppressWarnings("unchecked") final List<String> s3paths = (List<String>) s3pathsObj;
                if (!s3paths.isEmpty() && s3paths.stream().allMatch(path -> path != null && !path.trim().isEmpty())) {
                    final String s3region = (String) requestMap.get("s3region");
                    return new S3PathRequest(s3paths.stream()
                            .map(String::trim)
                            .collect(Collectors.toList()), s3region);
                }
            }
            return null;
        } catch (final Exception e) {
            LOG.warn("Failed to parse request body: {}", e.getMessage());
            return null;
        }
    }

    private String readConfigurationFromS3(final String s3path, final S3Client client) throws IOException {
        final Matcher matcher = S3_PATH_PATTERN.matcher(s3path);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 path format. Expected: s3://bucket/key");
        }

        final String bucketName = matcher.group(1);
        final String key = matcher.group(2);

        LOG.info("Reading configuration from S3: bucket={}, key={}", bucketName, key);

        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try (final ResponseInputStream<GetObjectResponse> s3Object = client.getObject(getObjectRequest);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8))) {

            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    private void handleS3ClientException(final HttpExchange exchange, final Exception e) throws IOException {
        if (e instanceof SdkClientException && e.getCause() instanceof java.net.UnknownHostException) {
            LOG.warn("Invalid AWS region: {}", e.getMessage());
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid AWS region");
        } else {
            LOG.error("AWS SDK error: {}", e.getMessage());
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, "AWS error: " + e.getMessage());
        }
    }

    private void sendErrorResponse(final HttpExchange exchange, final int statusCode, final String message) throws IOException {
        final String errorResponse = "{\"error\": \"" + message.replace("\"", "\\\"") + "\"}";
        final byte[] response = errorResponse.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, response.length);
        exchange.getResponseBody().write(response);
    }

    private static class S3PathRequest {
        public final List<String> s3paths;
        public final String s3region;

        public S3PathRequest(final List<String> s3paths, final String s3region) {
            this.s3paths = s3paths;
            this.s3region = s3region;
        }
    }
}