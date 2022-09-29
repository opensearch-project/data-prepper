/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace.model;

import org.opensearch.dataprepper.model.trace.Span;
import com.google.common.collect.Sets;

import java.util.Set;

public class SpanSet {

    private final Set<Span> spans;
    private final long timeSeen;

    public SpanSet() {
        this.spans = Sets.newConcurrentHashSet();
        this.timeSeen = System.currentTimeMillis();
    }

    public Set<Span> getSpans() {
        return spans;
    }

    public long getTimeSeen() {
        return timeSeen;
    }

    public void addSpan(final Span span) {
        spans.add(span);
    }
}
