/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.trace.Span;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class OtlpSignalTypeTest {

    @Test
    void testFromEvent_withSpan_returnsTrace() {
        final Span span = mock(Span.class);
        assertEquals(OtlpSignalType.TRACE, OtlpSignalType.fromEvent(span));
    }

    @Test
    void testFromEvent_withMetric_returnsMetric() {
        final Metric metric = mock(Metric.class);
        assertEquals(OtlpSignalType.METRIC, OtlpSignalType.fromEvent(metric));
    }

    @Test
    void testFromEvent_withLog_returnsLog() {
        final Log log = mock(Log.class);
        assertEquals(OtlpSignalType.LOG, OtlpSignalType.fromEvent(log));
    }

    @Test
    void testFromEvent_withUnknownEvent_returnsUnknown() {
        final Event unknownEvent = mock(Event.class);
        assertEquals(OtlpSignalType.UNKNOWN, OtlpSignalType.fromEvent(unknownEvent));
    }
}
