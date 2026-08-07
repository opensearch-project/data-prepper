/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

/**
 * A sum event in Data Prepper representing a metric event.
 * @since 1.4
 */
public interface Sum extends Metric {

    /**
     * Gets the value for a sum
     * @return the value
     * @since 1.4
     */
    Double getValue();

    /**
     * Gets the aggregation temporality for a sum
     * @return the aggregation temporality
     * @since 1.4
     */
    String getAggregationTemporality();

    /**
     * Gets if a sum is monotonic
     * @return if the sum is monotonic or not
     * @since 1.4
     */
    boolean isMonotonic();
}
