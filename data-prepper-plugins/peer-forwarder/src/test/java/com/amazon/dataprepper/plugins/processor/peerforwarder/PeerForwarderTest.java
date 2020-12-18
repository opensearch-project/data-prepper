package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.Clients;
import io.grpc.Channel;
import io.micrometer.core.instrument.Measurement;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PeerForwarder.class, TraceServiceGrpc.TraceServiceBlockingStub.class})
@PowerMockIgnore({"javax.net.ssl.*","javax.security.*"})
public class PeerForwarderTest {
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
    public void testProcessRequest() throws Exception {
        final PeerForwarder testPeerForwarder = generatePeerForwarder(Collections.singletonList(LOCAL_IP), 2);
        final TraceServiceGrpc.TraceServiceBlockingStub mockClient = mock(TraceServiceGrpc.TraceServiceBlockingStub.class);
        doReturn(null).when(mockClient).export(any());
        final List<Record<ExportTraceServiceRequest>> localBuffer = new ArrayList<>();
        final Object[] args1 = new Object[]{null, REQUEST_1, localBuffer};
        Whitebox.invokeMethod(testPeerForwarder, "processRequest", args1);
        Assert.assertEquals(1, localBuffer.size());
        Assert.assertEquals(REQUEST_1, localBuffer.get(0).getData());
        final Object[] args2 = new Object[]{mockClient, REQUEST_2, localBuffer};
        Whitebox.invokeMethod(testPeerForwarder, "processRequest", args2);
        Assert.assertEquals(1, localBuffer.size());
        verify(mockClient, times(1)).export(REQUEST_2);
    }

    @Test
    public void testSingleRemoteIpBothLocalAndForwardedRequest() throws Exception {
        final List<String> testIps = generateTestIps(2);
        final PeerForwarder testPeerForwarder = spy(generatePeerForwarder(testIps, 3));
        final Map<String, List<ExportTraceServiceRequest>> requestsByIp = testIps.stream()
                .collect(Collectors.toMap(ip-> ip, ip-> new ArrayList<>()));
        // Mock processRequest
        doAnswer(invocation -> {
            final TraceServiceGrpc.TraceServiceBlockingStub client = invocation.getArgument(0);
            final ExportTraceServiceRequest request = invocation.getArgument(1);
            if (client != null) {
                final String host =  client.getChannel().authority().split(":")[0];
                requestsByIp.get(host).add(request);
            } else {
                requestsByIp.get(LOCAL_IP).add(request);
            }
            return null;
        }).when(testPeerForwarder, "processRequest",
                any(),
                any(ExportTraceServiceRequest.class),
                any(List.class));

        testPeerForwarder.doExecute(Arrays.asList(new Record<>(REQUEST_1), new Record<>(REQUEST_2)));

        final List<ResourceSpans> expectedLocalResourceSpans = Arrays.asList(
                generateResourceSpans(SPAN_1, SPAN_2),
                generateResourceSpans(SPAN_3)
        );
        final List<ResourceSpans> expectedForwardedResourceSpans = Arrays.asList(
                generateResourceSpans(SPAN_5, SPAN_6),
                generateResourceSpans(SPAN_4)
        );
        Assert.assertEquals(1, requestsByIp.get(testIps.get(0)).size());
        final ExportTraceServiceRequest localRequest = requestsByIp.get(testIps.get(0)).get(0);
        final List<ResourceSpans> localResourceSpans = localRequest.getResourceSpansList();
        Assert.assertTrue(localResourceSpans.containsAll(expectedLocalResourceSpans));
        Assert.assertTrue(expectedLocalResourceSpans.containsAll(localResourceSpans));
        Assert.assertEquals(1, requestsByIp.get(testIps.get(1)).size());
        final ExportTraceServiceRequest forwardedRequest = requestsByIp.get(testIps.get(1)).get(0);
        final List<ResourceSpans> forwardedResourceSpans = forwardedRequest.getResourceSpansList();
        Assert.assertTrue(forwardedResourceSpans.containsAll(expectedForwardedResourceSpans));
        Assert.assertTrue(expectedForwardedResourceSpans.containsAll(forwardedResourceSpans));
    }

    @Test
    public void testSingleRemoteIpLocalRequestOnly() throws Exception {
        final List<String> testIps = generateTestIps(2);
        final PeerForwarder testPeerForwarder = spy(generatePeerForwarder(testIps, 3));
        final Map<String, List<ExportTraceServiceRequest>> requestsByIp = testIps.stream()
                .collect(Collectors.toMap(ip-> ip, ip-> new ArrayList<>()));
        // Mock processRequest
        doAnswer(invocation -> {
            final TraceServiceGrpc.TraceServiceBlockingStub client = invocation.getArgument(0);
            final ExportTraceServiceRequest request = invocation.getArgument(1);
            if (client != null) {
                final String host =  client.getChannel().authority().split(":")[0];
                requestsByIp.get(host).add(request);
            } else {
                requestsByIp.get(LOCAL_IP).add(request);
            }
            return null;
        }).when(testPeerForwarder, "processRequest",
                any(),
                any(ExportTraceServiceRequest.class),
                any(List.class));

        testPeerForwarder.doExecute(Collections.singletonList(new Record<>(REQUEST_3)));

        final List<ResourceSpans> expectedLocalResourceSpans = Collections.singletonList(
                generateResourceSpans(SPAN_1, SPAN_2, SPAN_3));
        Assert.assertEquals(1, requestsByIp.get(testIps.get(0)).size());
        final ExportTraceServiceRequest localRequest = requestsByIp.get(testIps.get(0)).get(0);
        final List<ResourceSpans> localResourceSpans = localRequest.getResourceSpansList();
        Assert.assertTrue(localResourceSpans.containsAll(expectedLocalResourceSpans));
        Assert.assertTrue(expectedLocalResourceSpans.containsAll(localResourceSpans));
        Assert.assertEquals(0, requestsByIp.get(testIps.get(1)).size());
    }

    @Test
    public void testSingleRemoteIpForwardedRequestOnly() throws Exception {
        final List<String> testIps = generateTestIps(2);
        final PeerForwarder testPeerForwarder = spy(generatePeerForwarder(testIps, 3));
        final Map<String, List<ExportTraceServiceRequest>> requestsByIp = testIps.stream()
                .collect(Collectors.toMap(ip-> ip, ip-> new ArrayList<>()));
        // Mock processRequest
        doAnswer(invocation -> {
            final TraceServiceGrpc.TraceServiceBlockingStub client = invocation.getArgument(0);
            final ExportTraceServiceRequest request = invocation.getArgument(1);
            if (client != null) {
                final String host =  client.getChannel().authority().split(":")[0];
                requestsByIp.get(host).add(request);
            } else {
                requestsByIp.get(LOCAL_IP).add(request);
            }
            return null;
        }).when(testPeerForwarder, "processRequest",
                any(),
                any(ExportTraceServiceRequest.class),
                any(List.class));

        testPeerForwarder.doExecute(Collections.singletonList(new Record<>(REQUEST_4)));

        final List<ResourceSpans> expectedForwardedResourceSpans = Collections.singletonList(
                generateResourceSpans(SPAN_4, SPAN_5, SPAN_6));
        Assert.assertEquals(1, requestsByIp.get(testIps.get(1)).size());
        final ExportTraceServiceRequest forwardedRequest = requestsByIp.get(testIps.get(1)).get(0);
        final List<ResourceSpans> forwardedResourceSpans = forwardedRequest.getResourceSpansList();
        Assert.assertTrue(forwardedResourceSpans.containsAll(expectedForwardedResourceSpans));
        Assert.assertTrue(expectedForwardedResourceSpans.containsAll(forwardedResourceSpans));
        Assert.assertEquals(0, requestsByIp.get(testIps.get(0)).size());
    }

    @Test
    public void testSingleRemoteIpForwardRequestError() {
        final List<String> testIps = generateTestIps(2);
        final Channel channel = mock(Channel.class);
        final PeerClientPool peerClientPool = mock(PeerClientPool.class);
        final TraceServiceGrpc.TraceServiceBlockingStub client = mock(TraceServiceGrpc.TraceServiceBlockingStub.class);
        when(channel.authority()).thenReturn(String.format("%s:21890", testIps.get(1)));
        when(peerClientPool.getClient(anyString())).thenReturn(client);
        when(client.export(any(ExportTraceServiceRequest.class))).thenThrow(new RuntimeException());
        when(client.getChannel()).thenReturn(channel);

        try (final MockedStatic<PeerClientPool> peerClientPoolClassMock = Mockito.mockStatic(PeerClientPool.class)) {
            peerClientPoolClassMock.when(PeerClientPool::getInstance).thenReturn(peerClientPool);

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
            // TODO: Verify metrics
        }
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
        return new PeerForwarder(new PluginSetting("peer_forwarder", settings) {{setPipelineName("testPipeline");}});
    }
}