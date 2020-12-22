package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.StaticPeerListProvider;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DataPrepperPlugin(name = "peer_forwarder", type = PluginType.PROCESSOR)
public class PeerForwarder extends AbstractPrepper<Record<ExportTraceServiceRequest>, Record<ExportTraceServiceRequest>> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarder.class);

    private final HashRing hashRing;
    private final PeerClientPool peerClientPool;
    private final int maxNumSpansPerRequest;

    public PeerForwarder(final PluginSetting pluginSetting,
                         final PeerClientPool peerClientPool,
                         final HashRing hashRing,
                         final int maxNumSpansPerRequest) {
        super(pluginSetting);
        this.peerClientPool = peerClientPool;
        this.hashRing = hashRing;
        this.maxNumSpansPerRequest = maxNumSpansPerRequest;
    }

    public PeerForwarder(final PluginSetting pluginSetting) {
        this(pluginSetting, PeerForwarderConfig.buildConfig(pluginSetting));
    }

    public PeerForwarder(final PluginSetting pluginSetting, final PeerForwarderConfig peerForwarderConfig) {
        this(
                pluginSetting,
                peerForwarderConfig.getPeerClientPool(),
                peerForwarderConfig.getHashRing(),
                peerForwarderConfig.getMaxNumSpansPerRequest()
        );
    }

    @Override
    public List<Record<ExportTraceServiceRequest>> doExecute(final Collection<Record<ExportTraceServiceRequest>> records) {
        final Map<String, List<ResourceSpans>> groupedRS = new HashMap<>();

        // Group ResourceSpans by consistent hashing of traceId
        for (final Record<ExportTraceServiceRequest> record : records) {
            for (final ResourceSpans rs : record.getData().getResourceSpansList()) {
                final List<Map.Entry<String, ResourceSpans>> rsBatch = PeerForwarderUtils.splitByTrace(rs);
                for (final Map.Entry<String, ResourceSpans> entry : rsBatch) {
                    final String traceId = entry.getKey();
                    final ResourceSpans newRS = entry.getValue();
                    final String dataPrepperIp = hashRing.getServerIp(traceId).orElse(StaticPeerListProvider.LOCAL_ENDPOINT);
                    groupedRS.computeIfAbsent(dataPrepperIp, x -> new ArrayList<>()).add(newRS);
                }
            }
        }

        // Buffer of requests to be exported to the downstream of the local data-prepper
        final List<Record<ExportTraceServiceRequest>> results = new ArrayList<>();

        for (final Map.Entry<String, List<ResourceSpans>> entry : groupedRS.entrySet()) {
            final TraceServiceGrpc.TraceServiceBlockingStub client;
            if (isAddressDefinedLocally(entry.getKey())) {
                client = null;
            } else {
                client = peerClientPool.getClient(entry.getKey());
            }

            // Create ExportTraceRequest for storing single batch of spans
            ExportTraceServiceRequest.Builder currRequestBuilder = ExportTraceServiceRequest.newBuilder();
            int currSpansCount = 0;
            for (final ResourceSpans rs : entry.getValue()) {
                final int rsSize = PeerForwarderUtils.getResourceSpansSize(rs);
                if (currSpansCount >= maxNumSpansPerRequest) {
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

    private boolean isAddressDefinedLocally(final String address) {
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

    @Override
    public void shutdown() {
        //TODO: cleanup resources
    }
}
