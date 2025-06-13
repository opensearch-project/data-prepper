/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationS3FileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
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
            final String requestBody = new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            final S3PathRequest s3PathRequest = parseS3PathRequest(requestBody);
            PipelinesDataFlowModel targetPipelinesDataFlowModel =
                    new PipelinesDataflowModelParser(
                            new PipelineConfigurationS3FileReader(s3PathRequest.s3paths, s3PathRequest.s3region)
                    ).parseConfiguration();

            PipelinesDataFlowModel currentPipelinesDataFlowModel = pipelinesProvider.getPipelinesDataFlowModel();
            // See the feasibility of dynamically updating current pipelinesDataFlowModel with target pipelinesDataFlowModel
            boolean dynamicUpdateFeasible =
                    DynamicPipelineUpdateUtil.isDynamicUpdateFeasible(
                            currentPipelinesDataFlowModel, targetPipelinesDataFlowModel);
            if (!dynamicUpdateFeasible) {
                sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "Dynamic update not feasible");
                return;
            }

            // TODO: Validate and update pipeline configuration
            // For now, we'll just log the contents and return success
            LOG.debug("Number of pipelines found : {}", targetPipelinesDataFlowModel.getPipelines().size());

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
            final Region s3region = Region.of((String) requestMap.get("s3region"));
            if (!Region.regions().contains(s3region)) {
                throw new IllegalArgumentException("Invalid region provided in the request body");
            }

            if (s3pathsObj instanceof List) {
                @SuppressWarnings("unchecked") final List<String> s3paths = (List<String>) s3pathsObj;
                if (!s3paths.isEmpty() && s3paths.stream().allMatch(path -> path != null && !path.trim().isEmpty())) {
                    return new S3PathRequest(s3paths.stream()
                            .map(String::trim)
                            .collect(Collectors.toList()), s3region);
                }
            }
            throw new IllegalArgumentException("Expecting to receive S3 paths along with the S3 region in the payload ");
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse request body: " + e.getMessage());
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
        public final Region s3region;

        public S3PathRequest(final List<String> s3paths, final Region s3region) {
            this.s3paths = s3paths;
            this.s3region = s3region;
        }
    }
}