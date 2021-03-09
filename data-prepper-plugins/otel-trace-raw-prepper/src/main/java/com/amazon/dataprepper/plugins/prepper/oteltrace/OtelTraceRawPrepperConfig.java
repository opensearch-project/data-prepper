package com.amazon.dataprepper.plugins.prepper.oteltrace;

public class OtelTraceRawPrepperConfig {
    static final String GC_INTERVAL = "gc_interval";
    static final long DEFAULT_GC_INTERVAL_MS = 30000L;
    static final String PARENT_SPAN_FLUSH_DELAY = "parent_span_flush_delay";
    static final long DEFAULT_PARENT_SPAN_FLUSH_DELAY_MS = 5000L;
}
