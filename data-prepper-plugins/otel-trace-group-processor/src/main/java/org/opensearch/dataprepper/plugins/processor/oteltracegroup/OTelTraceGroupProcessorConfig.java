/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;

import java.util.List;

public class OTelTraceGroupProcessorConfig {
    protected static final String TRACE_ID_FIELD = "traceId";
    protected static final String SPAN_ID_FIELD = "spanId";
    protected static final String PARENT_SPAN_ID_FIELD = "parentSpanId";
    protected static final String RAW_INDEX_ALIAS = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
    protected static final String STRICT_DATE_TIME = "strict_date_time";

    @JsonUnwrapped
    private ConnectionConfiguration esConnectionConfig;

    @JsonProperty("indices")
    @JsonPropertyDescription("The OpenSearch indices to query when looking up trace group information. " +
            "Supports concrete index names, aliases, and index patterns such as otel-v1-apm-span-*. " +
            "Defaults to the OpenTelemetry trace analytics raw alias.")
    @NotEmpty
    private List<@NotEmpty String> indices = List.of(RAW_INDEX_ALIAS);

    public ConnectionConfiguration getEsConnectionConfig() {
        return esConnectionConfig;
    }

    public List<String> getIndices() {
        return indices;
    }
}
