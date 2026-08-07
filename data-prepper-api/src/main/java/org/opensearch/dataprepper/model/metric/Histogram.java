/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

import java.util.List;

/**
 * A Histogram event
 *
 * @since 1.4
 */
public interface Histogram extends Metric {

    /**
     * Gets the sum for the histogram
     *
     * @return the sum of the values in the population
     * @since 1.4
     */
    Double getSum();

    /**
     * Gets the min for the histogram
     *
     * @return the min of the values in the population
     * @since 2.1
     */
    Double getMin();

    /**
     * Gets the max for the histogram
     *
     * @return the max of the values in the population
     * @since 2.1
     */
    Double getMax();

    /**
     * Gets the count of the histogram
     * @return the count, must be equal to the sum of the "count" fields in buckets
     * @since 1.4
     */
    Long getCount();

    /**
     * Gets the bucket count for the histogram
     *
     * @return the bucket count
     * @since 1.4
     */
    Integer getBucketCount();

    /**
     * Gets the number of explicit bounds for the histogram
     *
     * @return the number of bounds
     * @since 1.4
     */
    Integer getExplicitBoundsCount();

    /**
     * Gets the aggregation temporality for the histogram
     *
     * @return the aggregation temporality
     * @since 1.4
     */
    String getAggregationTemporality();

    /**
     * Gets the computed buckets for a histogram
     *
     * @return the buckets
     * @since 1.4
     */
    List<? extends Bucket> getBuckets();

    /**
     * Gets bucket counts for a histogram
     *
     * @return the bucket values
     * @since 1.4
     */
    List<Long> getBucketCountsList();

    /**
     * Gets the explicit bounds list for a histogram
     *
     * @return the bounds
     * @since 1.4
     */
    List<Double> getExplicitBoundsList();
}
