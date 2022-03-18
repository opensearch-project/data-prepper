/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.oteltrace;

public class OtelTraceRawProcessorConfig {
    static final String TRACE_FLUSH_INTERVAL = "trace_flush_interval";
    static final long DEFAULT_TG_FLUSH_INTERVAL_SEC = 180L;
    static final long DEFAULT_TRACE_ID_TTL_SEC = 15L;
    static final long MAX_TRACE_ID_CACHE_SIZE = 1000_000L;
}
