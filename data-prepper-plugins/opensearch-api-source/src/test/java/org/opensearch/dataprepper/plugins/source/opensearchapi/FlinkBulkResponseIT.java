/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

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
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.lenient;

/**
 * Integration test: sends a real BulkRequest via RestHighLevelClient
 * (same client Flink uses) to the opensearch_api source endpoint
 * and verifies the empty-items response is parsed without errors.
 *
 * This proves end-to-end compatibility with the Flink OpenSearch connector.
 */
@ExtendWith(MockitoExtension.class)
class FlinkBulkResponseIT {

    private static final int BUFFER_SIZE = 100;
    private static final int TIMEOUT_MS = 10_000;

    @Mock
    private PipelineDescription pipelineDescription;

    private Server server;
    private RestHighLevelClient restHighLevelClient;
    private int serverPort;

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");

        MetricsTestUtil.initMetrics();
        PluginMetrics pluginMetrics = PluginMetrics.fromNames("opensearch_api", "test-pipeline");

        BlockingBuffer<Record<Event>> buffer = new BlockingBuffer<>(BUFFER_SIZE, 8, "test-pipeline");

        OpenSearchAPIService service = new OpenSearchAPIService(TIMEOUT_MS, buffer, pluginMetrics);

        ServerBuilder sb = Server.builder();
        sb.http(0);
        sb.annotatedService(service, new HttpRequestExceptionHandler(pluginMetrics));
        server = sb.build();
        server.start().join();
        serverPort = server.activeLocalPort();

        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", serverPort, "http")));
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
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("test-index")
                .id("doc-1")
                .source(Map.of("message", "hello from flink", "timestamp", System.currentTimeMillis()), XContentType.JSON));
        bulkRequest.add(new IndexRequest("test-index")
                .id("doc-2")
                .source(Map.of("message", "second document", "count", 42), XContentType.JSON));

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        assertThat(response, is(notNullValue()));
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems().length, equalTo(0));
        assertThat(response.getTook(), is(notNullValue()));
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

        assertThat(response, is(notNullValue()));
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getItems().length, equalTo(0));
    }
}
