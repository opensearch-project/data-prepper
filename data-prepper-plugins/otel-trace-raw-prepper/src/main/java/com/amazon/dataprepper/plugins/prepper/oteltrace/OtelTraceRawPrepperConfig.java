package com.amazon.dataprepper.plugins.prepper.oteltrace;

public class OtelTraceRawPrepperConfig {
    static final String TRACE_FLUSH_INTERVAL = "trace_flush_interval";
    static final long DEFAULT_TG_FLUSH_INTERVAL = 180L;
    static final String ROOT_SPAN_FLUSH_DELAY = "root_span_flush_delay";
    static final long DEFAULT_ROOT_SPAN_FLUSH_DELAY = 30L;
    static final long DEFAULT_TRACE_ID_TTL = 300L;
    static final long MAX_TRACE_ID_CACHE_SIZE_PER_THREAD = 10_0000L;
}
