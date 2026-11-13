/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils;

import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Node;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.NodeOperationDetail;

import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.ToIntFunction;

/**
 * De-duplicates NodeOperationDetails by identity, like a HashSet, but merges the descriptive
 * keyAttributes (platform keys) of a duplicate into the kept element instead of dropping them. So a
 * service seen in two clusters, or as a Deployment and its canary, in one window keeps every value
 * (bounded per key) rather than whichever span was processed first.
 */
public class MergingNodeDetailSet extends AbstractSet<NodeOperationDetail> {
    private final Map<NodeOperationDetail, NodeOperationDetail> details = new HashMap<>();
    private final ToIntFunction<String> maxValuesPerKey;

    /** @param maxValuesPerKey the most distinct values a descriptive key may hold */
    public MergingNodeDetailSet(final ToIntFunction<String> maxValuesPerKey) {
        this.maxValuesPerKey = maxValuesPerKey;
    }

    @Override
    public boolean add(final NodeOperationDetail detail) {
        final NodeOperationDetail existing = details.putIfAbsent(detail, detail);
        if (existing == null) {
            return true;
        }
        merge(existing.getSourceNode(), detail.getSourceNode());
        merge(existing.getTargetNode(), detail.getTargetNode());
        return false;
    }

    private void merge(final Node into, final Node from) {
        if (into != null && from != null && into.getKeyAttributes() != null) {
            into.getKeyAttributes().mergeDescriptiveAttributes(from.getKeyAttributes(), maxValuesPerKey);
        }
    }

    @Override
    public boolean contains(final Object o) {
        return details.containsKey(o);
    }

    @Override
    public Iterator<NodeOperationDetail> iterator() {
        return details.keySet().iterator();
    }

    @Override
    public int size() {
        return details.size();
    }
}
