/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.integration.log;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.protobuf.ByteString;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

public class EndToEndOtelLogsSourceGrpcTest {
    private static final int SOURCE_PORT = 2021;
    private static final String INDEX_NAME = "otel-logs-index";

    private final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();
    final RestHighLevelClient openSearchClient = createOpenSearchClient();

    @Test
    public void testGrpcLogIngestion() {
        ingestLogs();

        searchForLogsAndAssert();
    }

    private void ingestLogs() {
        LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(String.format("gproto+http://127.0.0.1:%s/", SOURCE_PORT))
                .decorator(RetryingClient.newDecorator(RetryRule.failsafe()))
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        ExportLogsServiceRequest exportLogsServiceRequest = ExportLogsServiceRequest.newBuilder().addResourceLogs(
                ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setBody(AnyValue.newBuilder().setStringValue(apacheLogFaker.generateRandomCommonApacheLog()).build())
                                        .setSeverityNumberValue(1)
                                        .setTimeUnixNano(System.currentTimeMillis() * 1_000_000)
                                        .setTraceId(ByteString.copyFromUtf8("trace-id"))
                                        .setSpanId(ByteString.copyFromUtf8("span-id")
                                        )).build()
                        )
        ).build();

        ExportLogsServiceResponse response = client.export(exportLogsServiceRequest);
        assertNotNull(response);
    }

    private RestHighLevelClient createOpenSearchClient() {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        builder.withInsecure(true);
        return builder.build().createClient(null);
    }

    private void searchForLogsAndAssert() {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices();
                    final SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
                    searchRequest.source(SearchSourceBuilder.searchSource().size(100));
                    final SearchResponse searchResponse = openSearchClient.search(searchRequest, RequestOptions.DEFAULT);
                    assertEquals(1, getLogsFromSearchHits(searchResponse.getHits()).size());
                }
        );
    }
    private List<Map<String, Object>> getLogsFromSearchHits(final SearchHits searchHits) {
        return Arrays.stream(searchHits.getHits())
                .map(SearchHit::getSourceAsMap).collect(Collectors.toList());
    }

    private void refreshIndices() throws IOException {
        openSearchClient.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
    }
}
