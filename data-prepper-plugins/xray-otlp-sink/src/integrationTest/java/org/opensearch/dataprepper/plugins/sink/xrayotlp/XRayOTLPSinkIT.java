/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.JacksonStandardSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

class XRayOTLPSinkIT {
    @Test
    void testSinkProcessesHardcodedSpan() {
        final Span testSpan = JacksonStandardSpan.builder()
                .withTraceId("abc123")
                .withSpanId("def456")
                .withParentSpanId("parent-testSpan-id")
                .withName("my-test-testSpan")
                .withStartTime(String.valueOf(Instant.now()))
                .withEndTime(String.valueOf(Instant.now().plusMillis(10)))
                .withAttributes(Collections.emptyMap())
                .withKind("test")
                .build();

        final Record<Span> record = new Record<>(testSpan);
        final XRayOTLPSink sink = new XRayOTLPSink();

        sink.initialize();
        sink.output(List.of(record));
        sink.shutdown();
    }
}
