/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.trace.DefaultTraceGroupFields;
import com.amazon.dataprepper.model.trace.JacksonSpan;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.plugins.otel.codec.OTelProtoCodec;
import io.grpc.Channel;
import io.micrometer.core.instrument.Measurement;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.amazon.dataprepper.plugins.otel.codec.OTelProtoCodec.convertUnixNanosToISO8601;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeerForwarderTest {
    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final String LOCAL_IP = "127.0.0.1";
    private static final String TEST_TRACE_ID_1 = "b1";
    private static final String TEST_TRACE_ID_2 = "b2";
    private static final String TEST_SERVICE_A = "serviceA";
    private static final String TEST_SERVICE_B = "serviceB";
    private static final String TEST_SPAN_ID_1 = "d1";
    private static final String TEST_SPAN_ID_2 = "d2";
    private static final String TEST_SPAN_ID_3 = "d3";
    private static final String TEST_SPAN_ID_4 = "d4";
    private static final String TEST_SPAN_ID_5 = "d5";
    private static final String TEST_SPAN_ID_6 = "d6";
    private static final Span SPAN_1 = getSpan(TEST_TRACE_ID_1, TEST_SPAN_ID_1, "", TEST_SERVICE_A, "spanName1",
            io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
    private static final Span SPAN_2 = getSpan(TEST_TRACE_ID_1, TEST_SPAN_ID_2, "", TEST_SERVICE_A, "spanName2",
            io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
    private static final Span SPAN_3 = getSpan(TEST_TRACE_ID_1, TEST_SPAN_ID_3, "", TEST_SERVICE_A, "spanName3",
            io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
    private static final Span SPAN_4 = getSpan(TEST_TRACE_ID_2, TEST_SPAN_ID_4, "", TEST_SERVICE_B, "spanName4",
            io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
    private static final Span SPAN_5 = getSpan(TEST_TRACE_ID_2, TEST_SPAN_ID_5, "", TEST_SERVICE_B, "spanName5",
            io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
    private static final Span SPAN_6 = getSpan(TEST_TRACE_ID_2, TEST_SPAN_ID_6, "", TEST_SERVICE_B, "spanName6",
            io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);

    private static final List<Span> TEST_SPANS_ALL = Arrays.asList(SPAN_1, SPAN_2, SPAN_3, SPAN_4, SPAN_5, SPAN_6);
    private static final List<Span> TEST_SPANS_A = Arrays.asList(SPAN_1, SPAN_2, SPAN_3);
    private static final List<Span> TEST_SPANS_B = Arrays.asList(SPAN_4, SPAN_5, SPAN_6);

    private MockedStatic<PeerClientPool> peerClientPoolClassMock;

    @Mock
    private OTelProtoCodec.OTelProtoEncoder oTelProtoEncoder;

    @Mock
    private PeerClientPool peerClientPool;

    @Mock
    private TraceServiceGrpc.TraceServiceBlockingStub client;

    @Mock
    private CompletableFuture<ExportTraceServiceRequest> completableFuture;

    @Before
    public void setUp() throws DecoderException, UnsupportedEncodingException {
        peerClientPoolClassMock = Mockito.mockStatic(PeerClientPool.class);
        peerClientPoolClassMock.when(PeerClientPool::getInstance).thenReturn(peerClientPool);
    }

    @After
    public void tearDown() {
        // Need to release static mock as otherwise it will remain active on the thread when running other tests
        peerClientPoolClassMock.close();
    }

    public static Span getSpan(final String traceId, final String spanId, final String parentId,
                               final String serviceName, final String spanName, final io.opentelemetry.proto.trace.v1.Span.SpanKind spanKind) {
        final long startTimeNanos = System.nanoTime();
        final long endTimeNanos = System.nanoTime();
        final String endTime = UUID.randomUUID().toString();
        JacksonSpan.Builder builder = JacksonSpan.builder()
                .withSpanId(spanId)
                .withTraceId(traceId)
                .withTraceState("")
                .withParentSpanId(parentId)
                .withName(spanName)
                .withServiceName(serviceName)
                .withKind(spanKind.name())
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withEndTime(convertUnixNanosToISO8601(endTimeNanos))
                .withTraceGroup(parentId.isEmpty()? null : spanName)
                .withDurationInNanos(endTimeNanos - startTimeNanos);
        if (parentId.isEmpty()) {
            builder.withTraceGroupFields(
                    DefaultTraceGroupFields.builder()
                            .withStatusCode(1)
                            .withDurationInNanos(500L)
                            .withEndTime(endTime)
                            .build()
            );
        } else {
            builder.withTraceGroupFields(
                    DefaultTraceGroupFields.builder().build());
        }
        return builder.build();
    }

    private String extractServiceName(final ResourceSpans resourceSpans) {
        return resourceSpans.getResource().getAttributesList().stream().filter(kv -> kv.getKey().equals("service.name"))
                .findFirst().get().getValue().getStringValue();
    }

    private void reflectivelySetEncoder(final PeerForwarder peerForwarder, final OTelProtoCodec.OTelProtoEncoder encoder)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = PeerForwarder.class.getDeclaredField("oTelProtoEncoder");
        try {
            field.setAccessible(true);
            field.set(peerForwarder, encoder);
        } finally {
            field.setAccessible(false);
        }
    }

    @Test
    public void testLocalIpOnly() {
        final PeerForwarder testPeerForwarder = generatePeerForwarder(Collections.singletonList(LOCAL_IP), 2);
        final List<Span> inputSpans = Arrays.asList(SPAN_1, SPAN_2, SPAN_4, SPAN_5);
        final List<Record<Span>> exportedRecords = testPeerForwarder.doExecute(
                inputSpans.stream().map(Record::new).collect(Collectors.toList()));
        assertEquals(4, exportedRecords.size());
        final List<Span> exportedSpans = exportedRecords.stream().map(Record::getData).collect(Collectors.toList());
        assertTrue(exportedSpans.containsAll(inputSpans));
        assertTrue(inputSpans.containsAll(exportedSpans));
    }

    @Test
    public void testSingleRemoteIpBothLocalAndForwardedRequest() throws DecoderException {
        final List<String> testIps = generateTestIps(2);
        final Channel channel = mock(Channel.class);
        final String peerIp = testIps.get(1);
        when(channel.authority()).thenReturn(String.format("%s:21890", peerIp));
        when(peerClientPool.getClient(peerIp)).thenReturn(client);
        when(client.getChannel()).thenReturn(channel);
        final Map<String, List<ExportTraceServiceRequest>> requestsByIp = testIps.stream()
                .collect(Collectors.toMap(ip-> ip, ip-> new ArrayList<>()));
        doAnswer(invocation -> {
            final ExportTraceServiceRequest exportTraceServiceRequest = invocation.getArgument(0);
            requestsByIp.get(peerIp).add(exportTraceServiceRequest);
            return null;
        }).when(client).export(any(ExportTraceServiceRequest.class));

        MetricsTestUtil.initMetrics();
        final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);

        final List<Record<Span>> exportedRecords = testPeerForwarder
                .doExecute(TEST_SPANS_ALL.stream().map(Record::new).collect(Collectors.toList()));

        final List<Span> expectedLocalSpans = Arrays.asList(SPAN_1, SPAN_2, SPAN_3);
        Assert.assertEquals(3, exportedRecords.size());
        final List<Span> localSpans = exportedRecords.stream().map(Record::getData).collect(Collectors.toList());
        assertTrue(localSpans.containsAll(expectedLocalSpans));
        assertTrue(expectedLocalSpans.containsAll(localSpans));
        Assert.assertEquals(1, requestsByIp.get(peerIp).size());
        final ExportTraceServiceRequest forwardedRequest = requestsByIp.get(peerIp).get(0);
        final List<ResourceSpans> forwardedResourceSpans = forwardedRequest.getResourceSpansList();
        assertEquals(3, forwardedResourceSpans.size());
        forwardedResourceSpans.forEach(rs -> {
            assertEquals(TEST_SERVICE_B, extractServiceName(rs));
            assertEquals(1, rs.getInstrumentationLibrarySpansCount());
            final InstrumentationLibrarySpans ils = rs.getInstrumentationLibrarySpans(0);
            assertEquals(1, ils.getSpansCount());
            final io.opentelemetry.proto.trace.v1.Span sp = ils.getSpans(0);
            assertEquals(TEST_TRACE_ID_2, Hex.encodeHexString(sp.getTraceId().toByteArray()));
        });
    }

    @Test
    public void testSingleRemoteIpLocalRequestOnly() throws Exception {
        final List<String> testIps = generateTestIps(2);
        final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);

        final List<Record<Span>> exportedRecords = testPeerForwarder
                .doExecute(TEST_SPANS_A.stream().map(Record::new).collect(Collectors.toList()));

        Assert.assertEquals(3, exportedRecords.size());
        final List<Span> localSpans = exportedRecords.stream().map(Record::getData).collect(Collectors.toList());
        assertTrue(localSpans.containsAll(TEST_SPANS_A));
        assertTrue(TEST_SPANS_A.containsAll(localSpans));
    }

    @Test
    public void testSingleRemoteIpForwardedRequestOnly() throws Exception {
        final List<String> testIps = generateTestIps(2);
        final Channel channel = mock(Channel.class);
        final String peerIp = testIps.get(1);
        final String fullPeerIp = String.format("%s:21890", peerIp);
        when(channel.authority()).thenReturn(fullPeerIp);
        when(peerClientPool.getClient(peerIp)).thenReturn(client);
        when(client.getChannel()).thenReturn(channel);
        final Map<String, List<ExportTraceServiceRequest>> requestsByIp = testIps.stream()
                .collect(Collectors.toMap(ip-> ip, ip-> new ArrayList<>()));
        doAnswer(invocation -> {
            final ExportTraceServiceRequest exportTraceServiceRequest = invocation.getArgument(0);
            requestsByIp.get(peerIp).add(exportTraceServiceRequest);
            return null;
        }).when(client).export(any(ExportTraceServiceRequest.class));

        MetricsTestUtil.initMetrics();
        final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);

        final List<Record<Span>> exportedRecords = testPeerForwarder
                .doExecute(TEST_SPANS_B.stream().map(Record::new).collect(Collectors.toList()));

        Assert.assertEquals(1, requestsByIp.get(peerIp).size());
        final ExportTraceServiceRequest forwardedRequest = requestsByIp.get(peerIp).get(0);
        final List<ResourceSpans> forwardedResourceSpans = forwardedRequest.getResourceSpansList();
        assertEquals(3, forwardedResourceSpans.size());
        forwardedResourceSpans.forEach(rs -> {
            assertEquals(TEST_SERVICE_B, extractServiceName(rs));
            assertEquals(1, rs.getInstrumentationLibrarySpansCount());
            final InstrumentationLibrarySpans ils = rs.getInstrumentationLibrarySpans(0);
            assertEquals(1, ils.getSpansCount());
            final io.opentelemetry.proto.trace.v1.Span sp = ils.getSpans(0);
            assertEquals(TEST_TRACE_ID_2, Hex.encodeHexString(sp.getTraceId().toByteArray()));
        });
        Assert.assertEquals(0, exportedRecords.size());

        // Verify metrics
        final List<Measurement> forwardRequestErrorMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(PeerForwarder.ERRORS).toString());
        Assert.assertEquals(1, forwardRequestErrorMeasurements.size());
        Assert.assertEquals(0.0, forwardRequestErrorMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(PeerForwarder.REQUESTS).toString());
        Assert.assertEquals(1, forwardRequestSuccessMeasurements.size());
        Assert.assertEquals(1.0, forwardRequestSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestLatencyMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(PeerForwarder.LATENCY).toString());
        Assert.assertEquals(3, forwardRequestLatencyMeasurements.size());
        // COUNT
        Assert.assertEquals(1.0, forwardRequestLatencyMeasurements.get(0).getValue(), 0);
        // TOTAL_TIME
        assertTrue(forwardRequestLatencyMeasurements.get(1).getValue() > 0.0);
        // MAX
        assertTrue(forwardRequestLatencyMeasurements.get(2).getValue() > 0.0);
    }

    @Test
    public void testSingleRemoteIpForwardRequestClientError() {
        final List<String> testIps = generateTestIps(2);
        final Channel channel = mock(Channel.class);
        final String peerIp = testIps.get(1);
        final String fullPeerIp = String.format("%s:21890", peerIp);
        when(channel.authority()).thenReturn(fullPeerIp);
        when(peerClientPool.getClient(peerIp)).thenReturn(client);
        when(client.export(any(ExportTraceServiceRequest.class))).thenThrow(new RuntimeException());
        when(client.getChannel()).thenReturn(channel);

        MetricsTestUtil.initMetrics();
        final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);

        final List<Record<Span>> exportedRecords = testPeerForwarder
                .doExecute(TEST_SPANS_B.stream().map(Record::new).collect(Collectors.toList()));

        Assert.assertEquals(3, exportedRecords.size());
        final List<Span> exportedSpans = exportedRecords.stream().map(Record::getData).collect(Collectors.toList());
        assertTrue(exportedSpans.containsAll(TEST_SPANS_B));
        assertTrue(TEST_SPANS_B.containsAll(exportedSpans));

        // Verify metrics
        final List<Measurement> forwardRequestErrorMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(PeerForwarder.ERRORS).toString());
        Assert.assertEquals(1, forwardRequestErrorMeasurements.size());
        Assert.assertEquals(1.0, forwardRequestErrorMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(PeerForwarder.REQUESTS).toString());
        Assert.assertEquals(1, forwardRequestSuccessMeasurements.size());
        Assert.assertEquals(1.0, forwardRequestSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestLatencyMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(PeerForwarder.LATENCY).toString());
        Assert.assertEquals(3, forwardRequestLatencyMeasurements.size());
        // COUNT
        Assert.assertEquals(1.0, forwardRequestLatencyMeasurements.get(0).getValue(), 0);
        // TOTAL_TIME
        assertTrue(forwardRequestLatencyMeasurements.get(1).getValue() > 0.0);
        // MAX
        assertTrue(forwardRequestLatencyMeasurements.get(2).getValue() > 0.0);
    }

    @Test
    public void testSingleRemoteIpForwardRequestFutureError() throws ExecutionException, InterruptedException {
        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            completableFutureMockedStatic.when(() -> CompletableFuture.supplyAsync(
                    ArgumentMatchers.<Supplier<ExportTraceServiceRequest>>any(), any(ExecutorService.class)))
                    .thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());
            final List<String> testIps = generateTestIps(2);
            final Channel channel = mock(Channel.class);
            final String peerIp = testIps.get(1);
            final String fullPeerIp = String.format("%s:21890", peerIp);
            when(channel.authority()).thenReturn(fullPeerIp);
            when(peerClientPool.getClient(peerIp)).thenReturn(client);
            when(client.getChannel()).thenReturn(channel);
            when(client.export(any(ExportTraceServiceRequest.class))).thenReturn(null);

            MetricsTestUtil.initMetrics();
            final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);

            final List<Record<Span>> exportedRecords = testPeerForwarder
                    .doExecute(TEST_SPANS_B.stream().map(Record::new).collect(Collectors.toList()));

            verify(completableFuture, times(1)).get();
            Assert.assertEquals(3, exportedRecords.size());
            final List<Span> exportedSpans = exportedRecords.stream().map(Record::getData).collect(Collectors.toList());
            assertTrue(exportedSpans.containsAll(TEST_SPANS_B));
            assertTrue(TEST_SPANS_B.containsAll(exportedSpans));
        }
    }

    @Test
    public void testSingleRemoteIpForwardRequestEncodeError() throws NoSuchFieldException, IllegalAccessException,
            DecoderException, UnsupportedEncodingException {
        final List<String> testIps = generateTestIps(2);
        final String peerIp = testIps.get(1);
        when(peerClientPool.getClient(peerIp)).thenReturn(client);

        MetricsTestUtil.initMetrics();
        final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);
        when(oTelProtoEncoder.convertToResourceSpans(any(Span.class))).thenThrow(new DecoderException());
        reflectivelySetEncoder(testPeerForwarder, oTelProtoEncoder);

        final List<Record<Span>> exportedRecords = testPeerForwarder
                .doExecute(TEST_SPANS_B.stream().map(Record::new).collect(Collectors.toList()));

        verifyNoInteractions(client);
        Assert.assertEquals(3, exportedRecords.size());
        final List<Span> exportedSpans = exportedRecords.stream().map(Record::getData).collect(Collectors.toList());
        assertTrue(exportedSpans.containsAll(TEST_SPANS_B));
        assertTrue(TEST_SPANS_B.containsAll(exportedSpans));
    }

    @Test
    public void testPrepareForShutdown() {
        final PeerForwarder peerForwarder = generatePeerForwarder(Collections.singletonList(LOCAL_IP), 2);

        peerForwarder.prepareForShutdown();

        assertTrue(peerForwarder.isReadyForShutdown());
    }

    /**
     * Generate specified number of test Ip addresses following the pattern 127.0.0.1, 128.0.0.1, ...
     */
    private List<String> generateTestIps(int num) {
        final String[] ipArray = LOCAL_IP.split("\\.");
        final List<String> results = new ArrayList<>();
        results.add(LOCAL_IP);
        for (int i = 1; i < num; i++) {
            ipArray[0] = String.valueOf((Integer.parseInt(ipArray[0]) + 1));
            results.add(String.join(".", ipArray));
        }
        return results;
    }

    private PeerForwarder generatePeerForwarder(final List<String> staticEndpoints, final int spansPerRequest) {
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.DISCOVERY_MODE, "STATIC");
        settings.put(PeerForwarderConfig.STATIC_ENDPOINTS, staticEndpoints);
        settings.put(PeerForwarderConfig.MAX_NUM_SPANS_PER_REQUEST, spansPerRequest);
        settings.put(PeerForwarderConfig.TIME_OUT, 300);
        settings.put(PeerForwarderConfig.SSL, false);
        final PluginSetting pluginSetting = new PluginSetting("peer_forwarder", settings);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);

        return new PeerForwarder(pluginSetting);
    }
}