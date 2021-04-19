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

package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.google.common.collect.Sets;

import java.util.Set;

public class RawSpanSet {

    private final Set<RawSpan> rawSpans;
    private final long timeSeen;

    public RawSpanSet() {
        this.rawSpans = Sets.newConcurrentHashSet();
        this.timeSeen = System.currentTimeMillis();
    }

    public Set<RawSpan> getRawSpans() {
        return rawSpans;
    }

    public long getTimeSeen() {
        return timeSeen;
    }

    public void addRawSpan(final RawSpan rawSpan) {
        rawSpans.add(rawSpan);
    }
}
