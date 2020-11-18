package com.amazon.situp.plugins.processor.peerforwarder;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.record.Record;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SitupPlugin(name = "peer_forwarder", type = PluginType.PROCESSOR)
public class PeerForwarder implements Processor<Record<ExportTraceServiceRequest>, Record<ExportTraceServiceRequest>> {

    private final PeerForwarderConfig peerForwarderConfig;

    private final List<String> peerIps;

    private String localIp;

    public static String getLocalPublicIp() throws IOException {
        // Find public IP address
        final URL url_name = new URL("http://bot.whatismyipaddress.com");

        final BufferedReader sc =
                new BufferedReader(new InputStreamReader(url_name.openStream()));

        return sc.readLine().trim();
    }

    public PeerForwarder(final PluginSetting pluginSetting) {
        peerForwarderConfig = PeerForwarderConfig.buildConfig(pluginSetting);
        peerIps = new ArrayList<>(peerForwarderConfig.getPeerIps());
        try {
            localIp = getLocalPublicIp();
            peerIps.add(localIp);
        } catch (IOException e) {
            // TODO: handle this error properly
            e.printStackTrace();
        }
        Collections.sort(peerIps);
    }

    @Override
    public Collection<Record<ExportTraceServiceRequest>> execute(final Collection<Record<ExportTraceServiceRequest>> records) {
        final Map<String, List<ResourceSpans>> peerGroupedRS = new HashMap<>();
        for (final String peerIp: peerIps) {
            peerGroupedRS.put(peerIp, new ArrayList<>());
        }
        for (final Record<ExportTraceServiceRequest> record: records) {
            for (final ResourceSpans rs: record.getData().getResourceSpansList()) {
                final Map<String, ResourceSpans> rsBatch = splitByTrace(rs);
                assert rsBatch != null;
                for (final Map.Entry<String, ResourceSpans> entry: rsBatch.entrySet()) {
                    final String traceId = entry.getKey();
                    final ResourceSpans newRS = entry.getValue();
                    final String peerIp = getHostByConsistentHashing(traceId);
                    peerGroupedRS.get(peerIp).add(newRS);
                }
            }
        }

        // TODO: send ExportTraceServiceRequest to designated peer
        return null;
    }

    private Map<String, ResourceSpans> splitByTrace(final ResourceSpans rs) {
        return null;
    }

    private String getHostByConsistentHashing(final String traceId) {
        return null;
    }
}
