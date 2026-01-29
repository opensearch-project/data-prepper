/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.model.internal;

import java.util.List;

/**
 * Helper class to hold histogram bucket data
 */
public class HistogramBuckets {
    public final List<Long> bucketCounts;
    public final List<Double> explicitBounds;

    public HistogramBuckets(final List<Long> bucketCounts, final List<Double> explicitBounds) {
        this.bucketCounts = bucketCounts;
        this.explicitBounds = explicitBounds;
    }
}
