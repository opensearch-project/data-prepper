/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration.log;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
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
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;

import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

public class EndToEndOtelLogsSourceTest {
    private static final int SOURCE_PORT = 2021;
    private static final String INDEX_NAME = "otel-logs-index";

    private final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();

    @Test
    public void testOtelLogsSourcePipelineEndToEnd() throws InvalidProtocolBufferException {
        sendHttpRequestToSourceAndAssertResponse(SOURCE_PORT, createOtelLogsRequest());
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();
        final List<Map<String, Object>> retrievedDocs = new ArrayList<>();

        makeRequestAndAssertResponse(restHighLevelClient, retrievedDocs);
    }

    private HttpData createOtelLogsRequest() throws InvalidProtocolBufferException {
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

        return HttpData.copyOf(JsonFormat.printer().print(exportLogsServiceRequest).getBytes());
    }

    private void makeRequestAndAssertResponse(RestHighLevelClient restHighLevelClient, List<Map<String, Object>> retrievedDocs) {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices(restHighLevelClient);
                    final SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
                    searchRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundSources = getSourcesFromSearchHits(searchResponse.getHits());
                    assertEquals(1, foundSources.size());
                    retrievedDocs.addAll(foundSources);
                }
        );
    }

    private RestHighLevelClient prepareOpenSearchRestHighLevelClient() {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        builder.withInsecure(true);
        return builder.build().createClient(null);
    }

    private void sendHttpRequestToSourceAndAssertResponse(final int port, final HttpData httpData) {
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority(String.format("127.0.0.1:%d", port))
                        .method(HttpMethod.POST)
                        .path("/otel-logs-pipeline/logs")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                httpData)
                .aggregate()
                .whenComplete((i, ex) -> {
                    assertThat(i.status(), is(HttpStatus.OK));
                    final List<String> headerKeys = i.headers()
                            .stream()
                            .map(Map.Entry::getKey)
                            .map(AsciiString::toString)
                            .collect(Collectors.toList());
                    assertThat("Response Header Keys", headerKeys, not(contains("server")));
                }).join();
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> {
            Map<String, Object> source = hit.getSourceAsMap();
            sources.add(source);
        });
        return sources;
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }
}
