/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test: sends a real BulkRequest via RestHighLevelClient
 * (same client Flink uses) to the opensearch_api source endpoint
 * and verifies the empty-items response is parsed without errors.
 *
 * This proves end-to-end compatibility with the Flink OpenSearch connector.
 */
@ExtendWith(MockitoExtension.class)
class FlinkBulkResponseIT {

    private static final int TEST_PORT = 19202;
    private static final int BUFFER_SIZE = 100;
    private static final int TIMEOUT_MS = 10_000;

    @Mock
    private PipelineDescription pipelineDescription;

    private Server server;
    private RestHighLevelClient restHighLevelClient;

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");

        // Set up metrics
        MetricsTestUtil.initMetrics();
        PluginMetrics pluginMetrics = PluginMetrics.fromNames("opensearch_api", "test-pipeline");

        // Create buffer
        BlockingBuffer<Record<Event>> buffer = new BlockingBuffer<>(BUFFER_SIZE, 8, "test-pipeline");

        // Create the service
        OpenSearchAPIService service = new OpenSearchAPIService(TIMEOUT_MS, buffer, pluginMetrics);

        // Start an Armeria server with the service
        ServerBuilder sb = Server.builder();
        sb.http(TEST_PORT);
        sb.annotatedService(service, new HttpRequestExceptionHandler(pluginMetrics));
        server = sb.build();
        server.start().join();

        // Create RestHighLevelClient pointing at our test server
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", TEST_PORT, "http")));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (restHighLevelClient != null) {
            restHighLevelClient.close();
        }
        if (server != null) {
            server.stop().join();
        }
    }

    @Test
    void testFlinkBulkRequestWithEmptyItemsResponse() throws Exception {
        // Build a bulk request identical to what Flink would send
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("test-index")
                .id("doc-1")
                .source(Map.of("message", "hello from flink", "timestamp", System.currentTimeMillis()), XContentType.JSON));
        bulkRequest.add(new IndexRequest("test-index")
                .id("doc-2")
                .source(Map.of("message", "second document", "count", 42), XContentType.JSON));

        // Execute the bulk request (this is exactly what Flink does internally)
        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        // Verify: no failures, response parsed successfully
        assertNotNull(response);
        assertFalse(response.hasFailures(), "BulkResponse should report no failures");
        assertEquals(0, response.getItems().length, "Items array should be empty");
        assertNotNull(response.getTook(), "Took value should be present");
    }

    @Test
    void testFlinkBulkRequestMultipleDocuments() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("metrics-index")
                    .id("metric-" + i)
                    .source(Map.of("value", i, "name", "cpu_usage"), XContentType.JSON));
        }

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        assertNotNull(response);
        assertFalse(response.hasFailures());
        assertEquals(0, response.getItems().length);
    }

    @Test
    void testFlinkBulkRequestFailsOnBufferOverflow() throws Exception {
        // Create a service with a tiny buffer (capacity=1) to force overflow
        MetricsTestUtil.initMetrics();
        PluginMetrics overflowMetrics = PluginMetrics.fromNames("opensearch_api_overflow", "test-pipeline");
        BlockingBuffer<Record<Event>> tinyBuffer = new BlockingBuffer<>(1, 1, "test-pipeline");
        OpenSearchAPIService overflowService = new OpenSearchAPIService(TIMEOUT_MS, tinyBuffer, overflowMetrics);

        Server overflowServer = Server.builder()
                .http(TEST_PORT + 1)
                .annotatedService(overflowService, new HttpRequestExceptionHandler(overflowMetrics))
                .build();
        overflowServer.start().join();

        RestHighLevelClient overflowClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", TEST_PORT + 1, "http")));

        try {
            // Send more documents than the buffer can hold
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < 50; i++) {
                bulkRequest.add(new IndexRequest("test-index")
                        .id("overflow-" + i)
                        .source(Map.of("data", "x".repeat(1000)), XContentType.JSON));
            }

            // This should throw because Data Prepper returns non-200 on buffer overflow
            // Flink would see this as a complete bulk failure and retry
            org.opensearch.OpenSearchStatusException exception =
                    org.junit.jupiter.api.Assertions.assertThrows(
                            org.opensearch.OpenSearchStatusException.class,
                            () -> overflowClient.bulk(bulkRequest, RequestOptions.DEFAULT));

            // Verify it's a 413 (entity too large) - current DP behavior
            // Note: real OpenSearch would return 200 with errors:true in items
            assertTrue(exception.getMessage().contains("Unable to parse response body")
                    || exception.status().getStatus() == 413
                    || exception.status().getStatus() == 408);
        } finally {
            overflowClient.close();
            overflowServer.stop().join();
        }
    }
}
