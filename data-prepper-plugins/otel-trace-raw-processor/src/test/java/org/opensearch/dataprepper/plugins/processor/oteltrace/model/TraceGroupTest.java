/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace.model;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TraceGroupTest {
    @Test
    void fromSpan_creates_expected_TraceGroup() {
        final String traceGroup = UUID.randomUUID().toString();
        final TraceGroupFields traceGroupFields = mock(TraceGroupFields.class);
        final Span span = mock(Span.class);
        when(span.getTraceGroup()).thenReturn(traceGroup);
        when(span.getTraceGroupFields()).thenReturn(traceGroupFields);

        final TraceGroup objectUnderTest = TraceGroup.fromSpan(span);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getTraceGroup(), equalTo(traceGroup));
        assertThat(objectUnderTest.getTraceGroupFields(), equalTo(traceGroupFields));
    }
}