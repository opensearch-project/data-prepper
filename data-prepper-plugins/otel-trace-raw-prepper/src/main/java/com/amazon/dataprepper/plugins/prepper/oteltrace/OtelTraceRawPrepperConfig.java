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

package com.amazon.dataprepper.plugins.prepper.oteltrace;

public class OtelTraceRawPrepperConfig {
    static final String TRACE_FLUSH_INTERVAL = "trace_flush_interval";
    static final long DEFAULT_TG_FLUSH_INTERVAL_SEC = 180L;
    static final String ROOT_SPAN_FLUSH_DELAY = "root_span_flush_delay";
    static final long DEFAULT_ROOT_SPAN_FLUSH_DELAY_SEC = 30L;
    static final long DEFAULT_TRACE_ID_TTL_SEC = 300L;
    static final long MAX_TRACE_ID_CACHE_SIZE = 1000_000L;
}
