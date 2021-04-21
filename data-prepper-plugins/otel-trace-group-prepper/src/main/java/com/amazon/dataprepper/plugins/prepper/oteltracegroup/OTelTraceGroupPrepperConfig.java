/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import com.amazon.dataprepper.plugins.sink.opensearch.IndexConstants;

public class OTelTraceGroupPrepperConfig {
    protected static final String TRACE_ID_FIELD = "traceId";
    protected static final String SPAN_ID_FIELD = "spanId";
    protected static final String PARENT_SPAN_ID_FIELD = "parentSpanId";
    protected static final String TRACE_GROUP_FIELD = "traceGroup";
    protected static final String RAW_INDEX_ALIAS = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);

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
