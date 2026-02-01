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

import static com.linecorp.armeria.common.MediaType.JSON_UTF_8;
import static com.linecorp.armeria.common.MediaType.PROTOBUF;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

public class EndToEndOtelLogsSourceTest {
    private static final int SOURCE_PORT = 2021;
    private static final String INDEX_NAME = "otel-logs-index";

    private final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();
    final RestHighLevelClient openSearchClient = createOpenSearchClient();

    @Test
    public void testOtelLogsSourcePipelineEndToEnd() throws InvalidProtocolBufferException {
        ingestLogs("/otel-logs-pipeline/logs", createOtelLogsJsonRequest(), JSON_UTF_8);

        searchForLogsAndAssert();
    }

    @Test
    public void testOtelLogsSourceWithUnframedRequestsPipelineEndToEnd() throws InvalidProtocolBufferException {
        ingestLogs("/opentelemetry.proto.collector.logs.v1.LogsService/Export", createOtelLogsJsonRequest(), JSON_UTF_8);

        searchForLogsAndAssert();
    }

    @Test
    public void testOtelLogsSourcePipelineWithProtobufPayloadEndToEnd() throws InvalidProtocolBufferException {
        ingestLogs("/otel-logs-pipeline/logs", createOtelLogsProtobufRequest(), PROTOBUF);

        searchForLogsAndAssert();
    }


    private HttpData createOtelLogsJsonRequest() throws InvalidProtocolBufferException {
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

    private HttpData createOtelLogsProtobufRequest() throws InvalidProtocolBufferException {
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

        return HttpData.copyOf(exportLogsServiceRequest.toByteArray());
    }

    private void searchForLogsAndAssert() {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices();
                    final SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
                    searchRequest.source(SearchSourceBuilder.searchSource().size(100));
                    final SearchResponse searchResponse = openSearchClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundLogs = getLogsFromSearchHits(searchResponse.getHits());
                    assertEquals(1, foundLogs.size());
                }
        );
    }

    private RestHighLevelClient createOpenSearchClient() {
        return new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"))
                .withUsername("admin")
                .withPassword("admin")
                .withInsecure(true)
                .build()
                .createClient(null);
    }

    private void ingestLogs(String path, HttpData payload, MediaType mediaType) throws InvalidProtocolBufferException {
        RequestHeaders headers = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority(String.format("127.0.0.1:%d", SOURCE_PORT))
                .method(HttpMethod.POST)
                .path(path)
                .contentType(mediaType)
                .build();

        WebClient.of().execute(headers, payload)
                .aggregate()
                .whenComplete((i, ex) -> assertThat(i.status(), is(HttpStatus.OK)))
                .join();
    }

    private List<Map<String, Object>> getLogsFromSearchHits(final SearchHits searchHits) {
        return Arrays.stream(searchHits.getHits())
                .map(SearchHit::getSourceAsMap).collect(Collectors.toList());
    }

    private void refreshIndices() throws IOException {
        openSearchClient.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
    }
}
