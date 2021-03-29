package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.amazon.dataprepper.plugins.sink.elasticsearch.IndexConstants;

public class OtelTraceGroupPrepperConfig {
    protected static final String TRACE_ID_FIELD = "traceId";
    protected static final String SPAN_ID_FIELD = "spanId";
    protected static final String PARENT_SPAN_ID_FIELD = "parentSpanId";
    protected static final String TRACE_GROUP_NAME_FIELD = "traceGroup.name";
    protected static final String TRACE_GROUP_END_TIME_FIELD = "traceGroup.endTime";
    protected static final String TRACE_GROUP_STATUS_CODE_FIELD = "traceGroup.statusCode";
    protected static final String TRACE_GROUP_DURATION_IN_NANOS_FIELD = "traceGroup.durationInNanos";
    protected static final String RAW_INDEX_ALIAS = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);

    private final ConnectionConfiguration esConnectionConfig;

    public ConnectionConfiguration getEsConnectionConfig() {
        return esConnectionConfig;
    }

    private OtelTraceGroupPrepperConfig(final ConnectionConfiguration esConnectionConfig) {
        this.esConnectionConfig = esConnectionConfig;
    }

    public static OtelTraceGroupPrepperConfig buildConfig(final PluginSetting pluginSetting) {
        final ConnectionConfiguration esConnectionConfig = ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        return new OtelTraceGroupPrepperConfig(esConnectionConfig);
    }
}
