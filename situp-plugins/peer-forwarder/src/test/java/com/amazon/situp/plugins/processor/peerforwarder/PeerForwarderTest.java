package com.amazon.situp.plugins.processor.peerforwarder;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class PeerForwarderTest {

    private static final Span SPAN_1 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceId1")).setSpanId(ByteString.copyFromUtf8("spanId1")).build();
    private static final Span SPAN_2 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceId1")).setSpanId(ByteString.copyFromUtf8("spanId2")).build();
    private static final Span SPAN_3 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceId1")).setSpanId(ByteString.copyFromUtf8("spanId3")).build();
    private static final Span SPAN_4 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceId2")).setSpanId(ByteString.copyFromUtf8("spanId4")).build();
    private static final Span SPAN_5 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceId2")).setSpanId(ByteString.copyFromUtf8("spanId5")).build();
    private static final Span SPAN_6 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceId2")).setSpanId(ByteString.copyFromUtf8("spanId6")).build();

    private static final ExportTraceServiceRequest REQUEST_1 = generateRequest(SPAN_1, SPAN_2, SPAN_4);
    private static final ExportTraceServiceRequest REQUEST_2 = generateRequest(SPAN_3, SPAN_5, SPAN_6);

    private static ExportTraceServiceRequest generateRequest(final Span... spans) {
        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .setResource(Resource.newBuilder().build())
                        .addInstrumentationLibrarySpans(
                                InstrumentationLibrarySpans.newBuilder()
                                        .setInstrumentationLibrary(InstrumentationLibrary.newBuilder().build())
                                        .addAllSpans(Arrays.asList(spans)))
                        .build()).build();
    }

    private static ResourceSpans generateResourceSpans(final Span... spans) {
        return ResourceSpans.newBuilder().setResource(Resource.newBuilder().build()).addInstrumentationLibrarySpans(
                InstrumentationLibrarySpans.newBuilder()
                        .setInstrumentationLibrary(InstrumentationLibrary.newBuilder().build())
                        .addAllSpans(Arrays.asList(spans))).build();
    }

    @Test
    public void testLocalIpOnly() {
        final PeerForwarder testPeerForwarder = generatePeerForwarder(Collections.emptyList());
        final List<Record<ExportTraceServiceRequest>> exportedRecords =
                testPeerForwarder.execute(Arrays.asList(new Record<>(REQUEST_1), new Record<>(REQUEST_2)));
        Assert.assertTrue(exportedRecords.size() >= 3);
        final List<ResourceSpans> exportedResourceSpans = new ArrayList<>();
        for (final Record<ExportTraceServiceRequest> record: exportedRecords) {
            exportedResourceSpans.addAll(record.getData().getResourceSpansList());
        }
        final List<ResourceSpans> expectedResourceSpans = Arrays.asList(
                generateResourceSpans(SPAN_1, SPAN_2),
                generateResourceSpans(SPAN_3),
                generateResourceSpans(SPAN_4),
                generateResourceSpans(SPAN_5, SPAN_6)
        );
        Assert.assertTrue(exportedResourceSpans.containsAll(expectedResourceSpans) &&
                expectedResourceSpans.containsAll(exportedResourceSpans));
    }

    private PeerForwarder generatePeerForwarder(final List<String> peerIps) {
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.PEER_IPS, peerIps);
        settings.put(PeerForwarderConfig.MAX_NUM_SPANS_PER_REQUEST, 2);
        settings.put(PeerForwarderConfig.TIME_OUT, 300);
        return new PeerForwarder(new PluginSetting("peer_forwarder", settings));
    }
}