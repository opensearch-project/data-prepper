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
