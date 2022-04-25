/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import java.util.List;

public interface ExponentialHistogram extends Metric {

    /**
     * Gets the sum for the histogram
     *
     * @return the sum of the values in the population
     * @since 1.4
     */
    Double getSum();


    /**
     * Gets the count of the histogram
     * @return the count, must be equal to the sum of the "count" fields in buckets
     * @since 1.4
     */
    Long getCount();

    /**
     * Gets the aggregation temporality for the histogram
     *
     * @return the aggregation temporality
     * @since 1.4
     */
    String getAggregationTemporality();

    /**
     * Gets the positive range of exponential buckets
     *
     * @return the buckets
     * @since 1.4
     */
    List<? extends Bucket> getPositiveBuckets();

    /**
     * Gets the negative range of exponential buckets
     *
     * @return the buckets
     * @since 1.4
     */
    List<? extends Bucket> getNegativeBuckets();


    /**
     * Gets the positive range of exponential bucket counts
     *
     * @return the buckets
     * @since 1.4
     */
    List<Long> getNegative();


    /**
     * Gets the negative range of exponential bucket counts
     *
     * @return the buckets
     * @since 1.4
     */
    List<Long> getPositive();

    /**
     * Gets the zero count of events
     *
     * @return the zero count
     * @since 1.4
     */
    Long getZeroCount();

    /**
     * Gets the scale for the histogram
     *
     * @return the scale
     * @since 1.4
     */
    Integer getScale();

    /**
     * Gets the offset for negative buckets
     *
     * @return the offset
     * @since 1.4
     */
    Integer getNegativeOffset();


    /**
     * Gets the offset for positive buckets
     *
     * @return the offset
     * @since 1.4
     */
    Integer getPositiveOffset();
}