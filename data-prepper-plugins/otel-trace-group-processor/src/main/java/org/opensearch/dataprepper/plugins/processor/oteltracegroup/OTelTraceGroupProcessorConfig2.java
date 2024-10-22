package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;

public class OTelTraceGroupProcessorConfig2 {
    protected static final String TRACE_ID_FIELD = "traceId";
    protected static final String SPAN_ID_FIELD = "spanId";
    protected static final String PARENT_SPAN_ID_FIELD = "parentSpanId";
    protected static final String RAW_INDEX_ALIAS = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
    protected static final String STRICT_DATE_TIME = "strict_date_time";

    @JsonUnwrapped
    private ConnectionConfiguration2 esConnectionConfig;

    public ConnectionConfiguration2 getEsConnectionConfig() {
        return esConnectionConfig;
    }
}
