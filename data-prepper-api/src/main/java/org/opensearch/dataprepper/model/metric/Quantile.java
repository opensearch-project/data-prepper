/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

/**
 * Represents a quantile of a {@link Summary}.
 *
 * @since 1.4
 */
public interface Quantile {


    /**
     * Gets the value of the quantile
     * @return the value
     * @since 1.4
     */
    Double getValue();

    /**
     * Gets the quantile.
     * @return the quantile
     * @since 1.4
     */
    Double getQuantile();
}
