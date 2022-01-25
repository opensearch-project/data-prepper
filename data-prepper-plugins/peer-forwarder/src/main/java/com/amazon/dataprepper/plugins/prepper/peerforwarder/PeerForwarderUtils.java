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

package com.amazon.dataprepper.plugins.prepper.peerforwarder;

import com.amazon.dataprepper.model.trace.Span;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class PeerForwarderUtils {
    public static int getResourceSpansSize(final ResourceSpans rs) {
        return rs.getInstrumentationLibrarySpansList().stream().mapToInt(InstrumentationLibrarySpans::getSpansCount).sum();
    }

    public static Map<String, List<Span>> splitByTrace(final Collection<Span> spans) {
        return spans.stream().collect(Collectors.groupingBy(Span::getTraceId));
    }
}
