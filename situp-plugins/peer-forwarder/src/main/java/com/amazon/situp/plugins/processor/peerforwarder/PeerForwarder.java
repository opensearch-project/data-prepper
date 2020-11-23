package com.amazon.situp.plugins.processor.peerforwarder;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.record.Record;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.internal.shaded.bouncycastle.util.encoders.Hex;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SitupPlugin(name = "peer_forwarder", type = PluginType.PROCESSOR)
public class PeerForwarder implements Processor<Record<ExportTraceServiceRequest>, Record<ExportTraceServiceRequest>> {

    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarder.class);

    private final PeerForwarderConfig peerForwarderConfig;

    private final List<String> peerIps;

    private final Map<String, TraceServiceGrpc.TraceServiceBlockingStub> peerClients;

    public static String getLocalPublicIp() throws IOException {
        // Find public IP address
        final URL urlName = new URL("http://checkip.amazonaws.com/");

        final BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader(urlName.openStream()));

        return bufferedReader.readLine().trim();
    }

    public PeerForwarder(final PluginSetting pluginSetting) {
        peerForwarderConfig = PeerForwarderConfig.buildConfig(pluginSetting);
        peerIps = new ArrayList<>(new HashSet<>(peerForwarderConfig.getPeerIps()));
        final String localPublicIp;
        try {
            localPublicIp = getLocalPublicIp();
            peerIps.remove(localPublicIp);
        } catch (IOException e) {
            throw new RuntimeException("Cannot get localhost public IP.", e);
        }
        peerClients = peerIps.stream().collect(Collectors.toMap(ip-> ip, ip-> createGRPCClient(ip)));
        peerIps.add(localPublicIp);
        Collections.sort(peerIps);
    }

    private TraceServiceGrpc.TraceServiceBlockingStub createGRPCClient(final String ipAddress) {
        // TODO: replace hardcoded port with customization
        return Clients.builder(String.format("gproto+http://%s:21890/", ipAddress))
                .writeTimeout(Duration.ofSeconds(peerForwarderConfig.getTimeOut()))
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    }

    @Override
    public List<Record<ExportTraceServiceRequest>> execute(final Collection<Record<ExportTraceServiceRequest>> records) {
        final Map<String, List<ResourceSpans>> peerGroupedRS = new HashMap<>();
        for (final String peerIp: peerIps) {
            peerGroupedRS.put(peerIp, new ArrayList<>());
        }
        for (final Record<ExportTraceServiceRequest> record: records) {
            for (final ResourceSpans rs: record.getData().getResourceSpansList()) {
                final List<Map.Entry<String, ResourceSpans>> rsBatch = splitByTrace(rs);
                for (final Map.Entry<String, ResourceSpans> entry: rsBatch) {
                    final String traceId = entry.getKey();
                    final ResourceSpans newRS = entry.getValue();
                    final String peerIp = getHostByConsistentHashing(traceId);
                    peerGroupedRS.get(peerIp).add(newRS);
                }
            }
        }

        // Send ExportTraceServiceRequest to designated peer
        final List<Record<ExportTraceServiceRequest>> results = new ArrayList<>();
        for (final String peerIp: peerIps) {
            final TraceServiceGrpc.TraceServiceBlockingStub client = peerClients.getOrDefault(peerIp, null);
            ExportTraceServiceRequest.Builder currRequestBuilder = ExportTraceServiceRequest.newBuilder();
            int currSpansCount = 0;
            for (final ResourceSpans rs: peerGroupedRS.get(peerIp)) {
                final int rsSize = getResourceSpansSize(rs);
                if (currSpansCount >= peerForwarderConfig.getMaxNumSpansPerRequest()) {
                    final ExportTraceServiceRequest currRequest = currRequestBuilder.build();
                    processRequest(client, currRequest, results);
                    currRequestBuilder = ExportTraceServiceRequest.newBuilder();
                    currSpansCount = 0;
                }
                currRequestBuilder.addResourceSpans(rs);
                currSpansCount += rsSize;
            }
            // last request
            final ExportTraceServiceRequest currRequest = currRequestBuilder.build();
            processRequest(client, currRequest, results);
        }
        return results;
    }

    /**
     * Forward request to the peer address if client is given, otherwise push the request to local buffer.
     */
    private void processRequest(final TraceServiceGrpc.TraceServiceBlockingStub client,
                                final ExportTraceServiceRequest request,
                                final List<Record<ExportTraceServiceRequest>> localBuffer) {
        if (client != null) {
            try {
                client.export(request);
            } catch (Exception e) {
                LOG.error(String.format("Failed to forward the request:\n%s\n", request.toString()));
                localBuffer.add(new Record<>(request));
            }
        } else {
            localBuffer.add(new Record<>(request));
        }
    }

    private List<Map.Entry<String, ResourceSpans>> splitByTrace(final ResourceSpans rs) {
        final List<Map.Entry<String, ResourceSpans>> result = new ArrayList<>();
        for (final InstrumentationLibrarySpans ils: rs.getInstrumentationLibrarySpansList()) {
            final Map<String, ResourceSpans.Builder> batches = new HashMap<>();
            for (final Span span: ils.getSpansList()) {
                final String sTraceId = Hex.toHexString(span.getTraceId().toByteArray());
                if (!batches.containsKey(sTraceId)) {
                    final ResourceSpans.Builder newRSBuilder = ResourceSpans.newBuilder()
                            .setResource(rs.getResource());
                    newRSBuilder.addInstrumentationLibrarySpansBuilder().setInstrumentationLibrary(ils.getInstrumentationLibrary());
                    batches.put(sTraceId, newRSBuilder);
                }

                // there is only one instrumentation library per batch
                batches.get(sTraceId).getInstrumentationLibrarySpansBuilder(0).addSpans(span);
            }

            batches.forEach((traceId, rsBuilder) -> result.add(new AbstractMap.SimpleEntry<>(traceId, rsBuilder.build())));
        }

        return result;
    }

    private String getHostByConsistentHashing(final String traceId) {
        // TODO: better consistent hashing algorithm, e.g. ring hashing
        return peerIps.get(traceId.hashCode() % peerIps.size());
    }

    private int getResourceSpansSize(final ResourceSpans rs) {
        return rs.getInstrumentationLibrarySpansList().stream().mapToInt(InstrumentationLibrarySpans::getSpansCount).sum();
    }
}
