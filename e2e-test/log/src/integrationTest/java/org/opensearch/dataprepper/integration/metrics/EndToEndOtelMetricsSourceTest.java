/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.integration.metrics;

import static com.linecorp.armeria.common.MediaType.JSON_UTF_8;
import static com.linecorp.armeria.common.MediaType.PROTOBUF;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
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
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;


public class EndToEndOtelMetricsSourceTest {
    private static final int SOURCE_PORT = 2021;
    private static final String INDEX_NAME = "otel-metrics-index";
    private static final String HTTP_PATH = "/otel-metrics-pipeline/metrics";

    final RestHighLevelClient openSearchClient = createOpenSearchClient();

    @Test
    public void testOtelMetricsSourcePipelineHttpServiceEndToEnd() throws InvalidProtocolBufferException {
        ingestMetrics(HTTP_PATH, createOtelMetricsJsonRequest(), JSON_UTF_8);

        searchForMetricsAndAssert();
    }

    @Test
    public void testOtelMetricsSourcePipelineHttpServiceWithProtobufPayloadEndToEnd() throws InvalidProtocolBufferException {
        ingestMetrics(HTTP_PATH, createOtelMetricsProtobufRequest(), PROTOBUF);

        searchForMetricsAndAssert();
    }


    private HttpData createOtelMetricsJsonRequest() throws InvalidProtocolBufferException {
        return HttpData.copyOf(JsonFormat.printer().print(createMetricsServiceRequest()).getBytes());
    }

    private HttpData createOtelMetricsProtobufRequest() throws InvalidProtocolBufferException {
        return HttpData.copyOf(createMetricsServiceRequest().toByteArray());
    }

    private void searchForMetricsAndAssert() {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices();
                    final SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
                    searchRequest.source(SearchSourceBuilder.searchSource().size(100));
                    final SearchResponse searchResponse = openSearchClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundMetrics = getMetricsFromSearchHits(searchResponse.getHits());
                    assertEquals(1, foundMetrics.size());
                    final Map<String, Object> actualMetric = foundMetrics.get(0);
                    assertThat(createExpectedMetric().entrySet(), everyItem(is(in(actualMetric.entrySet()))));
                }
        );
    }

    private Map<String, Object> createExpectedMetric() {
        return Map.of(
                "kind", "GAUGE",
                "description", "description",
                "serviceName", "service",
                "resource.attributes.service@name", "service",
                "name", "name",
                "value", 4.0
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

    private void ingestMetrics(String path, HttpData payload, MediaType mediaType) throws InvalidProtocolBufferException {
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

    private List<Map<String, Object>> getMetricsFromSearchHits(final SearchHits searchHits) {
        return Arrays.stream(searchHits.getHits())
                .map(SearchHit::getSourceAsMap).collect(Collectors.toList());
    }

    private void refreshIndices() throws IOException {
        openSearchClient.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
    }

    public static ExportMetricsServiceRequest createMetricsServiceRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        Metric metric = Metric.newBuilder().setGauge(gauge).setUnit("seconds")
                .setName("name")
                .setDescription("description")
                .build();

        ScopeMetrics.Builder scopeMetric = ScopeMetrics.newBuilder()
                .addMetrics(metric)
                .setScope(InstrumentationScope.newBuilder()
                        .setName("scopeName")
                        .setVersion("scopeVersion")
                        .build());

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetric)
                .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics)
                .build();
    }
}
