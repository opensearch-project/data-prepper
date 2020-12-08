package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "peer_forwarder", type = PluginType.PROCESSOR)
public class PeerForwarder implements Processor<Record<ExportTraceServiceRequest>, Record<ExportTraceServiceRequest>> {

    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarder.class);

    private final PeerForwarderConfig peerForwarderConfig;

    private final List<String> dataPrepperIps;

    private final Map<String, TraceServiceGrpc.TraceServiceBlockingStub> peerClients;

    private final HashRing hashRing;

    public static boolean isAddressDefinedLocally(final String address) {
        final InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            return false;
        }
        if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress()) {
            return true;
        } else {
            try {
                return NetworkInterface.getByInetAddress(inetAddress) != null;
            } catch (SocketException e) {
                return false;
            }
        }
    }

    public PeerForwarder(final PluginSetting pluginSetting) {
        peerForwarderConfig = PeerForwarderConfig.buildConfig(pluginSetting);
        dataPrepperIps = new ArrayList<>(new HashSet<>(peerForwarderConfig.getDataPrepperIps()));
        peerClients = dataPrepperIps.stream().filter(ip -> !isAddressDefinedLocally(ip))
                .collect(Collectors.toMap(ip-> ip, ip-> createGRPCClient(ip)));
        hashRing = new HashRing(dataPrepperIps, PeerForwarderConfig.NUM_VIRTUAL_NODES);
    }

    private TraceServiceGrpc.TraceServiceBlockingStub createGRPCClient(final String ipAddress) {
        // TODO: replace hardcoded port with customization
        return Clients.builder(String.format("gproto+http://%s:21890/", ipAddress))
                .writeTimeout(Duration.ofSeconds(peerForwarderConfig.getTimeOut()))
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    }

    @Override
    public List<Record<ExportTraceServiceRequest>> execute(final Collection<Record<ExportTraceServiceRequest>> records) {
        final Map<String, List<ResourceSpans>> groupedRS = new HashMap<>();
        for (final String dataPrepperIp: dataPrepperIps) {
            groupedRS.put(dataPrepperIp, new ArrayList<>());
        }

        // Group ResourceSpans by consistent hashing of traceId
        for (final Record<ExportTraceServiceRequest> record: records) {
            for (final ResourceSpans rs: record.getData().getResourceSpansList()) {
                final List<Map.Entry<String, ResourceSpans>> rsBatch = PeerForwarderUtils.splitByTrace(rs);
                for (final Map.Entry<String, ResourceSpans> entry: rsBatch) {
                    final String traceId = entry.getKey();
                    final ResourceSpans newRS = entry.getValue();
                    final String dataPrepperIp = getHostByConsistentHashing(traceId);
                    groupedRS.get(dataPrepperIp).add(newRS);
                }
            }
        }

        // Buffer of requests to be exported to the downstream of the local data-prepper
        final List<Record<ExportTraceServiceRequest>> results = new ArrayList<>();
        for (final String dataPrepperIp: dataPrepperIps) {
            final TraceServiceGrpc.TraceServiceBlockingStub client = peerClients.getOrDefault(dataPrepperIp, null);
            // Create ExportTraceRequest for storing single batch of spans
            ExportTraceServiceRequest.Builder currRequestBuilder = ExportTraceServiceRequest.newBuilder();
            int currSpansCount = 0;
            for (final ResourceSpans rs: groupedRS.get(dataPrepperIp)) {
                final int rsSize = PeerForwarderUtils.getResourceSpansSize(rs);
                if (currSpansCount >= peerForwarderConfig.getMaxNumSpansPerRequest()) {
                    final ExportTraceServiceRequest currRequest = currRequestBuilder.build();
                    // Send the batch request to designated remote peer or ingest into localhost
                    processRequest(client, currRequest, results);
                    currRequestBuilder = ExportTraceServiceRequest.newBuilder();
                    currSpansCount = 0;
                }
                currRequestBuilder.addResourceSpans(rs);
                currSpansCount += rsSize;
            }
            // Dealing with the last batch request
            if (currSpansCount > 0) {
                final ExportTraceServiceRequest currRequest = currRequestBuilder.build();
                processRequest(client, currRequest, results);
            }
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

    private String getHostByConsistentHashing(final String traceId) {
        // TODO: better consistent hashing algorithm, e.g. ring hashing
        return hashRing.getServerIp(traceId.getBytes());
    }
}
