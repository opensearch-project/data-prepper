package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PeerForwarderConfig {
    public static final String DATA_PREPPER_IPS = "data_prepper_ips";
    public static final String TIME_OUT = "time_out";
    public static final String MAX_NUM_SPANS_PER_REQUEST = "span_agg_count";

    private final List<String> dataPrepperIps;

    private final int timeOut;

    private final int maxNumSpansPerRequest;

    public List<String> getDataPrepperIps() {
        return dataPrepperIps;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public int getMaxNumSpansPerRequest() {
        return maxNumSpansPerRequest;
    }

    private PeerForwarderConfig(final List<String> dataPrepperIps, final int timeOut, final int maxNumSpansPerRequest) {
        checkNotNull(dataPrepperIps, "peerIps cannot be null");
        this.dataPrepperIps = dataPrepperIps;
        this.timeOut = timeOut;
        this.maxNumSpansPerRequest = maxNumSpansPerRequest;
    }

    @SuppressWarnings("unchecked")
    public static PeerForwarderConfig buildConfig(final PluginSetting pluginSetting) {
        return new PeerForwarderConfig(
                (List<String>) pluginSetting.getAttributeOrDefault(DATA_PREPPER_IPS, new ArrayList<>()),
                pluginSetting.getIntegerOrDefault(TIME_OUT, 300),
                pluginSetting.getIntegerOrDefault(MAX_NUM_SPANS_PER_REQUEST, 48));
    }
}
