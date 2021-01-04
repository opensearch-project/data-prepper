package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.PeerListProvider;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.micrometer.core.instrument.Measurement;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeerForwarderTest {
    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final String LOCAL_IP = "127.0.0.1";
    private static final Span SPAN_1 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceIdA")).setSpanId(ByteString.copyFromUtf8("spanId1")).build();
    private static final Span SPAN_2 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceIdA")).setSpanId(ByteString.copyFromUtf8("spanId2")).build();
    private static final Span SPAN_3 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceIdA")).setSpanId(ByteString.copyFromUtf8("spanId3")).build();
    private static final Span SPAN_4 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceIdB")).setSpanId(ByteString.copyFromUtf8("spanId4")).build();
    private static final Span SPAN_5 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceIdB")).setSpanId(ByteString.copyFromUtf8("spanId5")).build();
    private static final Span SPAN_6 = Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("traceIdB")).setSpanId(ByteString.copyFromUtf8("spanId6")).build();

    private static final ExportTraceServiceRequest REQUEST_1 = generateRequest(SPAN_1, SPAN_2, SPAN_4);
    private static final ExportTraceServiceRequest REQUEST_2 = generateRequest(SPAN_3, SPAN_5, SPAN_6);
    private static final ExportTraceServiceRequest REQUEST_3 = generateRequest(SPAN_1, SPAN_2, SPAN_3);
    private static final ExportTraceServiceRequest REQUEST_4 = generateRequest(SPAN_4, SPAN_5, SPAN_6);

    private MockedStatic<PeerClientPool> peerClientPoolClassMock;

    @Mock
    private PeerClientPool peerClientPool;

    @Mock
    private TraceServiceGrpc.TraceServiceBlockingStub client;

    @Before
    public void setUp() {
        peerClientPoolClassMock = Mockito.mockStatic(PeerClientPool.class);
        peerClientPoolClassMock.when(PeerClientPool::getInstance).thenReturn(peerClientPool);
    }

    @After
    public void tearDown() {
        // Need to release static mock as otherwise it will remain active on the thread when running other tests
        peerClientPoolClassMock.close();
    }

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
        final PeerForwarder testPeerForwarder = generatePeerForwarder(Collections.singletonList(LOCAL_IP), 2);
        final List<Record<ExportTraceServiceRequest>> exportedRecords =
                testPeerForwarder.doExecute(Arrays.asList(new Record<>(REQUEST_1), new Record<>(REQUEST_2)));
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
        Assert.assertTrue(exportedResourceSpans.containsAll(expectedResourceSpans));
        Assert.assertTrue(expectedResourceSpans.containsAll(exportedResourceSpans));
    }

    @Test
    public void testSingleRemoteIpBothLocalAndForwardedRequest() {
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

        final List<Record<ExportTraceServiceRequest>> exportedRecords = testPeerForwarder
                .doExecute(Arrays.asList(new Record<>(REQUEST_1), new Record<>(REQUEST_2)));

        final List<ResourceSpans> expectedLocalResourceSpans = Arrays.asList(
                generateResourceSpans(SPAN_1, SPAN_2),
                generateResourceSpans(SPAN_3)
        );
        final List<ResourceSpans> expectedForwardedResourceSpans = Arrays.asList(
                generateResourceSpans(SPAN_5, SPAN_6),
                generateResourceSpans(SPAN_4)
        );
        Assert.assertEquals(1, exportedRecords.size());
        final ExportTraceServiceRequest localRequest = exportedRecords.get(0).getData();
        final List<ResourceSpans> localResourceSpans = localRequest.getResourceSpansList();
        Assert.assertTrue(localResourceSpans.containsAll(expectedLocalResourceSpans));
        Assert.assertTrue(expectedLocalResourceSpans.containsAll(localResourceSpans));
        Assert.assertEquals(1, requestsByIp.get(peerIp).size());
        final ExportTraceServiceRequest forwardedRequest = requestsByIp.get(peerIp).get(0);
        final List<ResourceSpans> forwardedResourceSpans = forwardedRequest.getResourceSpansList();
        Assert.assertTrue(forwardedResourceSpans.containsAll(expectedForwardedResourceSpans));
        Assert.assertTrue(expectedForwardedResourceSpans.containsAll(forwardedResourceSpans));
    }

    @Test
    public void testSingleRemoteIpLocalRequestOnly() throws Exception {
        final List<String> testIps = generateTestIps(2);
        final PeerForwarder testPeerForwarder = generatePeerForwarder(testIps, 3);

        final List<Record<ExportTraceServiceRequest>> exportedRecords = testPeerForwarder
                .doExecute(Collections.singletonList(new Record<>(REQUEST_3)));

        final List<ResourceSpans> expectedLocalResourceSpans = Collections.singletonList(
                generateResourceSpans(SPAN_1, SPAN_2, SPAN_3));
        Assert.assertEquals(1, exportedRecords.size());
        final ExportTraceServiceRequest localRequest = exportedRecords.get(0).getData();
        final List<ResourceSpans> localResourceSpans = localRequest.getResourceSpansList();
        Assert.assertTrue(localResourceSpans.containsAll(expectedLocalResourceSpans));
        Assert.assertTrue(expectedLocalResourceSpans.containsAll(localResourceSpans));
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

        final List<Record<ExportTraceServiceRequest>> exportedRecords = testPeerForwarder
                .doExecute(Collections.singletonList(new Record<>(REQUEST_4)));

        final List<ResourceSpans> expectedForwardedResourceSpans = Collections.singletonList(
                generateResourceSpans(SPAN_4, SPAN_5, SPAN_6));
        Assert.assertEquals(1, requestsByIp.get(peerIp).size());
        final ExportTraceServiceRequest forwardedRequest = requestsByIp.get(peerIp).get(0);
        final List<ResourceSpans> forwardedResourceSpans = forwardedRequest.getResourceSpansList();
        Assert.assertTrue(forwardedResourceSpans.containsAll(expectedForwardedResourceSpans));
        Assert.assertTrue(expectedForwardedResourceSpans.containsAll(forwardedResourceSpans));
        Assert.assertEquals(0, exportedRecords.size());

        // Verify metrics
        final List<Measurement> forwardRequestErrorMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(String.format("%s:%s", PeerForwarder.FORWARD_REQUEST_ERRORS_PREFIX, fullPeerIp)).toString());
        Assert.assertEquals(1, forwardRequestErrorMeasurements.size());
        Assert.assertEquals(0.0, forwardRequestErrorMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(String.format("%s:%s", PeerForwarder.FORWARD_REQUEST_SUCCESS_PREFIX, fullPeerIp)).toString());
        Assert.assertEquals(1, forwardRequestSuccessMeasurements.size());
        Assert.assertEquals(1.0, forwardRequestSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestLatencyMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(String.format("%s:%s", PeerForwarder.FORWARD_REQUEST_LATENCY_PREFIX, fullPeerIp)).toString());
        Assert.assertEquals(3, forwardRequestLatencyMeasurements.size());
        // COUNT
        Assert.assertEquals(1.0, forwardRequestLatencyMeasurements.get(0).getValue(), 0);
        // TOTAL_TIME
        Assert.assertTrue(forwardRequestLatencyMeasurements.get(1).getValue() > 0.0);
        // MAX
        Assert.assertTrue(forwardRequestLatencyMeasurements.get(2).getValue() > 0.0);
    }

    @Test
    public void testSingleRemoteIpForwardRequestError() {
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

        final List<Record<ExportTraceServiceRequest>> exportedRecords = testPeerForwarder
                .doExecute(Collections.singletonList(new Record<>(REQUEST_4)));

        final List<ResourceSpans> expectedLocalResourceSpans = Collections.singletonList(
                generateResourceSpans(SPAN_4, SPAN_5, SPAN_6));
        Assert.assertEquals(1, exportedRecords.size());
        final ExportTraceServiceRequest exportedRequest = exportedRecords.get(0).getData();
        final List<ResourceSpans> forwardedResourceSpans = exportedRequest.getResourceSpansList();
        Assert.assertTrue(forwardedResourceSpans.containsAll(expectedLocalResourceSpans));
        Assert.assertTrue(expectedLocalResourceSpans.containsAll(forwardedResourceSpans));

        // Verify metrics
        final List<Measurement> forwardRequestErrorMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(String.format("%s:%s", PeerForwarder.FORWARD_REQUEST_ERRORS_PREFIX, fullPeerIp)).toString());
        Assert.assertEquals(1, forwardRequestErrorMeasurements.size());
        Assert.assertEquals(1.0, forwardRequestErrorMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(String.format("%s:%s", PeerForwarder.FORWARD_REQUEST_SUCCESS_PREFIX, fullPeerIp)).toString());
        Assert.assertEquals(1, forwardRequestSuccessMeasurements.size());
        Assert.assertEquals(0.0, forwardRequestSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> forwardRequestLatencyMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add("peer_forwarder")
                        .add(String.format("%s:%s", PeerForwarder.FORWARD_REQUEST_LATENCY_PREFIX, fullPeerIp)).toString());
        Assert.assertEquals(3, forwardRequestLatencyMeasurements.size());
        // COUNT
        Assert.assertEquals(1.0, forwardRequestLatencyMeasurements.get(0).getValue(), 0);
        // TOTAL_TIME
        Assert.assertTrue(forwardRequestLatencyMeasurements.get(1).getValue() > 0.0);
        // MAX
        Assert.assertTrue(forwardRequestLatencyMeasurements.get(2).getValue() > 0.0);
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
        return new PeerForwarder(new PluginSetting("peer_forwarder", settings) {{setPipelineName(TEST_PIPELINE_NAME);}});
    }
}