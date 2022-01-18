/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import com.amazon.dataprepper.plugins.sink.opensearch.index.IndexConstants;
import com.amazon.dataprepper.plugins.sink.opensearch.index.IndexType;

public class OTelTraceGroupPrepperConfig {
    protected static final String TRACE_ID_FIELD = "traceId";
    protected static final String SPAN_ID_FIELD = "spanId";
    protected static final String PARENT_SPAN_ID_FIELD = "parentSpanId";
    protected static final String RAW_INDEX_ALIAS = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
    protected static final String STRICT_DATE_TIME = "strict_date_time";

    private final ConnectionConfiguration esConnectionConfig;

    public ConnectionConfiguration getEsConnectionConfig() {
        return esConnectionConfig;
    }

    private OTelTraceGroupPrepperConfig(final ConnectionConfiguration esConnectionConfig) {
        this.esConnectionConfig = esConnectionConfig;
    }

    public static OTelTraceGroupPrepperConfig buildConfig(final PluginSetting pluginSetting) {
        final ConnectionConfiguration esConnectionConfig = ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        return new OTelTraceGroupPrepperConfig(esConnectionConfig);
    }
}
