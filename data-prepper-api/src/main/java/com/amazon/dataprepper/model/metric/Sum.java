/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

/**
 * A sum event in Data Prepper representing a metric event.
 * @since 1.4
 */
public interface Sum extends Metric {

    /**
     * Gets the value for a gauge
     * @return the value
     * @since 1.4
     */
    Double getValue();

    /**
     * Gets the value for a gauge
     * @return the value
     * @since 1.4
     */
    String getAggregationTemporality();

    /**
     * Gets the value for a gauge
     * @return the value
     * @since 1.4
     */
    boolean isMonotonic();
}
