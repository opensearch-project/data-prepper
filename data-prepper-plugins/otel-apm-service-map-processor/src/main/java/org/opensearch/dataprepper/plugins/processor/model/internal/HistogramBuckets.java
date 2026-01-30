/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
