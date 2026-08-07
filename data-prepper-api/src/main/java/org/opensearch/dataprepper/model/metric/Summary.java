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
 * A summary, representing a metric event.
 * @since 1.4
 */
public interface Summary extends Metric {

    /**
     * Gets the quantiles for a summary
     * @return the quantiles
     * @since 1.4
     */
    List<? extends Quantile> getQuantiles();

    /**
     * Gets the number of quantiles for a summary
     * @return the number of quantiles
     * @since 1.4
     */
    Integer getQuantileValuesCount();

    /**
     * Gets the value for a summary
     * @return the sum
     * @since 1.4
     */
    Double getSum();

    /**
     * Gets the number of values in the population
     * @return the number of values
     * @since 1.4
     */
    Long getCount();
}
