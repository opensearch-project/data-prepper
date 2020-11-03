package com.amazon.situp.integration;


import com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.internal.shaded.bouncycastle.util.encoders.Hex;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class EndToEndRawSpanTest {

    private static final Random RANDOM = new Random();
    private static final List<Span.SpanKind> SPAN_KINDS =
            Arrays.asList(Span.SpanKind.SPAN_KIND_CLIENT, Span.SpanKind.SPAN_KIND_CONSUMER, Span.SpanKind.SPAN_KIND_INTERNAL, Span.SpanKind.SPAN_KIND_PRODUCER, Span.SpanKind.SPAN_KIND_SERVER);
    private static final String INDEX_NAME = "otel-v1-apm-span-000001";

    @Test
    public void testPipelineEndToEnd() throws IOException, InterruptedException {
        //Send data to otel trace source
        final ExportTraceServiceRequest exportTraceServiceRequest1 = getExportTraceServiceRequest(
                getRandomResourceSpans(10)
        );

        final ExportTraceServiceRequest exportTraceServiceRequest2 = getExportTraceServiceRequest(
                getRandomResourceSpans(10)
        );

        sendExportTraceServiceRequestToSource(exportTraceServiceRequest1);
        sendExportTraceServiceRequestToSource(exportTraceServiceRequest2);

        //Verify data in elasticsearch sink
        final List<Map<String, Object>> expectedDocuments = getExpectedDocuments(exportTraceServiceRequest1, exportTraceServiceRequest2);
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        final RestHighLevelClient restHighLevelClient = builder.build().createClient();
        // Wait for data to flow through pipeline and be indexed by ES
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices(restHighLevelClient);
                    final SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
                    searchRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundSources = getSourcesFromSearchHits(searchResponse.getHits());
                    Assert.assertEquals(expectedDocuments.size(), foundSources.size());
                    /**
                     * Our raw trace processor add more fields than the actual sent object. These are defaults from the proto.
                     * So assertion is done if all the expected fields exists.
                     *
                     * TODO: Can we do better?
                     *
                     */
                    expectedDocuments.forEach(expectedDoc -> {
                        Assert.assertTrue(foundSources.stream()
                                .filter(i -> i.get("traceId").equals(expectedDoc.get("traceId")))
                                .findFirst().get()
                                .entrySet().containsAll(expectedDoc.entrySet()));
                    });
                }
        );
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private void sendExportTraceServiceRequestToSource(ExportTraceServiceRequest request) {
        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.newClient(
                "gproto+http://127.0.0.1:21890/", TraceServiceGrpc.TraceServiceBlockingStub.class);
        client.export(request);
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> sources.add(hit.getSourceAsMap()));
        return sources;
    }


    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind, final int statusCode, final String statusMessage) {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        final Status.Builder statusBuilder = Status.newBuilder().setCodeValue(statusCode);
        if (statusMessage != null) {
            statusBuilder.setMessage(statusMessage);
        }
        final Status status = statusBuilder.build();
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
                                                .setStatus(status)
                                                .setTraceId(ByteString.copyFrom(traceId))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(List<ResourceSpans> spans){
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(spans)
                .build();
    }

    private List<Map<String, Object>> getExpectedDocuments(ExportTraceServiceRequest...exportTraceServiceRequests) {
        final List<Map<String, Object>> expectedDocuments = new ArrayList<>();
        for(int i=0; i<exportTraceServiceRequests.length; i++) {
            exportTraceServiceRequests[i].getResourceSpansList().forEach( resourceSpans -> {
                final String resourceName = getServiceName(resourceSpans);
                resourceSpans.getInstrumentationLibrarySpansList().forEach( instrumentationLibrarySpans -> {
                    instrumentationLibrarySpans.getSpansList().forEach(span -> {
                        expectedDocuments.add(getExpectedEsDocumentSource(span, resourceName));
                    });
                });
            });
        }
        return expectedDocuments;
    }

    private Map<String, Object> getExpectedEsDocumentSource(final Span span, final String serviceName) {
        final Map<String, Object> esDocSource = new HashMap<>();
        esDocSource.put("traceId", Hex.toHexString(span.getTraceId().toByteArray()));
        esDocSource.put("spanId", Hex.toHexString(span.getSpanId().toByteArray()));
        esDocSource.put("parentSpanId", Hex.toHexString(span.getParentSpanId().toByteArray()));
        esDocSource.put("name", span.getName());
        esDocSource.put("kind", span.getKind().name());
        esDocSource.put("status.code", span.getStatus().getCodeValue());
        esDocSource.put("status.message", span.getStatus().getMessage());
        esDocSource.put("serviceName", serviceName);
        return esDocSource;
    }

    private byte[] getRandomBytes(int len) {
        final byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private List<ResourceSpans> getRandomResourceSpans(int len) throws UnsupportedEncodingException {
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        for(int i=0; i<len; i++) {
            spansList.add(
                    getResourceSpans(
                            UUID.randomUUID().toString(),
                            UUID.randomUUID().toString(),
                            getRandomBytes(8),
                            getRandomBytes(8),
                            getRandomBytes(16),
                            SPAN_KINDS.get(RANDOM.nextInt(SPAN_KINDS.size())),
                            RANDOM.nextInt(17),
                            UUID.randomUUID().toString()
                    )
            );
        }
        return spansList;
    }

    private String getServiceName(final ResourceSpans resourceSpans) {
        return resourceSpans.getResource().getAttributesList().stream().filter(kv -> kv.getKey().equals("service.name"))
                .findFirst().get().getValue().getStringValue();
    }
}
