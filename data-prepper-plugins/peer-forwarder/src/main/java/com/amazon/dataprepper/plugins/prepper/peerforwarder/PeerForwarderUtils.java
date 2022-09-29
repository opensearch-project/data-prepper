/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder;

import org.opensearch.dataprepper.model.trace.Span;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class PeerForwarderUtils {

    public static Map<String, List<Span>> splitByTrace(final Collection<Span> spans) {
        return spans.stream().collect(Collectors.groupingBy(Span::getTraceId));
    }
}
