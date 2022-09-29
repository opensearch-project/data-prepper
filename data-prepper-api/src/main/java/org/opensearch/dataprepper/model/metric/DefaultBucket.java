/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

/**
 * The default implementation of {@link Bucket}, measurement within a {@link Histogram}
 *
 * @since 1.4
 */
public class DefaultBucket implements Bucket {

    private Double min;
    private Double max;
    private Long count;

    // required for serialization
    DefaultBucket() {}

    public DefaultBucket(Double min, Double max, Long count) {
        this.min = min;
        this.max = max;
        this.count = count;
    }

    @Override
    public Double getMin() {
        return min;
    }

    @Override
    public Double getMax() {
        return max;
    }

    @Override
    public Long getCount() {
        return count;
    }
}