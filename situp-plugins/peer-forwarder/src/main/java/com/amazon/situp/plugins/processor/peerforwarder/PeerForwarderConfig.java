package com.amazon.situp.plugins.processor.peerforwarder;

import com.amazon.situp.model.configuration.PluginSetting;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PeerForwarderConfig {
    public static final String PEER_IPS = "peer_ips";
    public static final String TIME_OUT = "time_out";
    public static final String MAX_NUM_SPANS_PER_REQUEST = "span_agg_count";

    private final List<String> peerIps;

    private final int timeOut;

    private final int maxNumSpansPerRequest;

    public List<String> getPeerIps() {
        return peerIps;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public int getMaxNumSpansPerRequest() {
        return maxNumSpansPerRequest;
    }

    private PeerForwarderConfig(final List<String> peerIps, final int timeOut, final int maxNumSpansPerRequest) {
        checkNotNull(peerIps, "peerIps cannot be null");
        this.peerIps = peerIps;
        this.timeOut = timeOut;
        this.maxNumSpansPerRequest = maxNumSpansPerRequest;
    }

    @SuppressWarnings("unchecked")
    public static PeerForwarderConfig buildConfig(final PluginSetting pluginSetting) {
        return new PeerForwarderConfig(
                (List<String>) pluginSetting.getAttributeOrDefault(PEER_IPS, new ArrayList<>()),
                pluginSetting.getIntegerOrDefault(TIME_OUT, 300),
                pluginSetting.getIntegerOrDefault(MAX_NUM_SPANS_PER_REQUEST, 48));
    }
}
