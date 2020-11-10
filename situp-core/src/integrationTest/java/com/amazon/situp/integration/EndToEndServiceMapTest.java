package com.amazon.situp.integration;


import com.google.protobuf.ByteString;
import com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class EndToEndServiceMapTest {

    private final Map<String, ResourceSpans> spanIdToRS = new HashMap<>();
    private final List<Map<String, Object>> possibleEdges = new ArrayList<>();
    private static final List<ServiceMapTestData> testDataSet1 = Arrays.asList(
            ServiceMapTestData.DATA_100, ServiceMapTestData.DATA_200, ServiceMapTestData.DATA_300, ServiceMapTestData.DATA_400,
            ServiceMapTestData.DATA_500, ServiceMapTestData.DATA_600, ServiceMapTestData.DATA_700, ServiceMapTestData.DATA_800,
            ServiceMapTestData.DATA_900, ServiceMapTestData.DATA_1000, ServiceMapTestData.DATA_1100);
    private static final List<ServiceMapTestData> testDataSet2 = Arrays.asList(
            ServiceMapTestData.DATA_101, ServiceMapTestData.DATA_201, ServiceMapTestData.DATA_301, ServiceMapTestData.DATA_401,
            ServiceMapTestData.DATA_501);
    private static final String SERVICE_MAP_INDEX_NAME = "otel-v1-apm-service-map";

    @Test
    public void testPipelineEndToEnd() throws IOException, InterruptedException {
        // Send test trace group 1
        final ExportTraceServiceRequest exportTraceServiceRequest1 = getExportTraceServiceRequest(
                getResourceSpansBatch("ABC", testDataSet1)
        );

        sendExportTraceServiceRequestToSource(exportTraceServiceRequest1);

        //Verify data in elasticsearch sink
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        final RestHighLevelClient restHighLevelClient = builder.build().createClient();

        // Wait for service map processor by 2 * window_duration
        Thread.sleep(6000);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final List<Map<String, Object>> foundSources = getSourcesFromIndex(restHighLevelClient, SERVICE_MAP_INDEX_NAME);
                    foundSources.forEach(source -> source.remove("hashId"));
                    Assert.assertEquals(8, foundSources.size());
                    Assert.assertTrue(foundSources.containsAll(possibleEdges) && possibleEdges.containsAll(foundSources));
                }
        );

        // Resend the same batch of spans (No new edges should be created)
        sendExportTraceServiceRequestToSource(exportTraceServiceRequest1);
        // Send test trace group 2
        final ExportTraceServiceRequest exportTraceServiceRequest2 = getExportTraceServiceRequest(
                getResourceSpansBatch("CBA", testDataSet2)
        );
        sendExportTraceServiceRequestToSource(exportTraceServiceRequest2);

        // Wait for service map processor by 2 * window_duration
        Thread.sleep(6000);
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final List<Map<String, Object>> foundSources = getSourcesFromIndex(restHighLevelClient, SERVICE_MAP_INDEX_NAME);
                    foundSources.forEach(source -> source.remove("hashId"));
                    Assert.assertEquals(12, foundSources.size());
                    Assert.assertTrue(foundSources.containsAll(possibleEdges) && possibleEdges.containsAll(foundSources));
                }
        );
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private void sendExportTraceServiceRequestToSource(final ExportTraceServiceRequest request) {
        Clients.newClient(
                "gproto+http://127.0.0.1:21890/", TraceServiceGrpc.TraceServiceBlockingStub.class).export(request);
    }

    private List<Map<String, Object>> getSourcesFromIndex(final RestHighLevelClient restHighLevelClient, final String index) throws IOException {
        refreshIndices(restHighLevelClient);
        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(
                SearchSourceBuilder.searchSource().size(100)
        );
        final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        return getSourcesFromSearchHits(searchResponse.getHits());
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> sources.add(hit.getSourceAsMap()));
        return sources;
    }


    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind) {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("service.name")
                                        .setValue(AnyValue.newBuilder().setStringValue(serviceName).build()).build())
                                .build()
                )
                .addInstrumentationLibrarySpans(
                        0,
                        InstrumentationLibrarySpans.newBuilder()
                                .addSpans(
                                        Span.newBuilder()
                                                .setName(spanName)
                                                .setKind(spanKind)
                                                .setSpanId(ByteString.copyFrom(spanId))
                                                .setParentSpanId(parentSpanId)
                                                .setTraceId(ByteString.copyFrom(traceId))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(List<ResourceSpans> spans) {
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(spans)
                .build();
    }

    private List<ResourceSpans> getResourceSpansBatch(final String traceId, final List<ServiceMapTestData> data) {
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        for(int i=0; i < data.size(); i++) {
            final ServiceMapTestData currData = data.get(i);
            final String parentId = currData.parentId;
            final String spanId = currData.spanId;
            final String serviceName = currData.serviceName;
            final String spanName = currData.name;
            final Span.SpanKind spanKind = currData.spanKind;
            final ResourceSpans rs = getResourceSpans(
                    serviceName,
                    spanName,
                    spanId.getBytes(),
                    parentId != null ? parentId.getBytes() : null,
                    traceId.getBytes(),
                    spanKind
            );
            spansList.add(rs);
            spanIdToRS.put(spanId, rs);
            if (parentId != null) {
                ResourceSpans parentRS = spanIdToRS.get(parentId);
                String parentServiceName = parentRS.getResource().getAttributes(0).getValue().getStringValue();
                if (!parentServiceName.equals(serviceName)) {
                    String rootSpanName = getRootSpanName(parentId);
                    Map<String, Object> destination = new HashMap<>();
                    destination.put("resource", spanName);
                    destination.put("domain", serviceName);
                    Map<String, Object> edge = new HashMap<>();
                    edge.put("serviceName", parentServiceName);
                    edge.put("kind", parentRS.getInstrumentationLibrarySpans(0).getSpans(0).getKind().name());
                    edge.put("traceGroupName", rootSpanName);
                    edge.put("destination", destination);
                    edge.put("target", null);
                    possibleEdges.add(edge);

                    Map<String, Object> target = new HashMap<>(destination);
                    edge = new HashMap<>();
                    edge.put("serviceName", serviceName);
                    edge.put("kind", spanKind.name());
                    edge.put("traceGroupName", rootSpanName);
                    edge.put("destination", null);
                    edge.put("target", target);
                    possibleEdges.add(edge);
                }
            }
        }
        return spansList;
    }

    private String getRootSpanName(String spanId) {
        ResourceSpans rs = spanIdToRS.get(spanId);
        ByteString parentSpanId = rs.getInstrumentationLibrarySpans(0).getSpans(0).getParentSpanId();
        while (!parentSpanId.isEmpty()) {
            rs = spanIdToRS.get(parentSpanId.toStringUtf8());
            parentSpanId = rs.getInstrumentationLibrarySpans(0).getSpans(0).getParentSpanId();
        }
        return rs.getInstrumentationLibrarySpans(0).getSpans(0).getName();
    }
}
