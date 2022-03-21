/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

import java.util.List;

/**
 * A Histogram event
 *
 * @since 1.4
 */
public interface Histogram extends Metric {

    /**
     * Gets the sum for the histogram
     * @return the sum of the values in the population
     * @since 1.4
     */
    Double getSum();

    /**
     * Gets the bucket count for the histogram
     * @return the bucket count
     * @since 1.4
     */
    Integer getBucketCount();

    /**
     * Gets the number of explicit bounds for the histogram
     * @return the number of bounds
     * @since 1.4
     */
    Integer getExplicitBoundsCount();

    /**
     * Gets the aggregation temporality for the histogram
     * @return the aggregation temporality
     * @since 1.4
     */
    String getAggregationTemporality();

    /**
     * Gets the actual buckets for a histogram
     * @return the buckets
     * @since 1.4
     */
    List<JacksonHistogram.Bucket> getBuckets();

}
